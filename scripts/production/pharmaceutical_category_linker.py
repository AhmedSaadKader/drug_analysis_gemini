import logging
from pathlib import Path
import traceback
import psycopg2
from datetime import datetime
import time
from typing import List, Dict, Tuple
import json
import gemini_api
import config


class PharmaceuticalCategoryLinker:
    def __init__(self, batch_size: int = 50):
        """Initialize the PharmaceuticalCategoryLinker.

        Args:
            batch_size: Number of ingredients to process in each batch
        """
        print(f"Initializing PharmaceuticalCategoryLinker with batch_size={batch_size}")
        self.batch_size = batch_size

        try:
            # Create logs directory if it doesn't exist
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            print("Created logs directory")

            # Generate timestamp for log files
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Setup main logger
            self.logger = logging.getLogger("CategoryLinker")
            self.logger.setLevel(logging.INFO)

            # Add file handler for main logger
            main_log_file = log_dir / f"category_linker_{timestamp}.log"
            main_handler = logging.FileHandler(main_log_file)
            main_formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s"
            )
            main_handler.setFormatter(main_formatter)
            self.logger.addHandler(main_handler)

            # Add console handler for main logger
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter("%(levelname)s: %(message)s")
            console_handler.setFormatter(console_formatter)
            self.logger.addHandler(console_handler)

            # Setup changes logger
            self.changes_logger = logging.getLogger("CategoryChanges")
            self.changes_logger.setLevel(logging.INFO)

            # Add file handler for changes logger
            changes_log_file = log_dir / f"category_changes_{timestamp}.log"
            changes_handler = logging.FileHandler(changes_log_file)
            changes_handler.setFormatter(main_formatter)
            self.changes_logger.addHandler(changes_handler)

            # Log initial messages
            self.logger.info(f"Main log file: {main_log_file}")
            self.logger.info(f"Changes log file: {changes_log_file}")
            self.logger.info("PharmaceuticalCategoryLinker initialized successfully")

        except Exception as e:
            print(f"ERROR: Failed to initialize logging: {e}")
            import traceback

            traceback.print_exc()
            raise

    def get_categories(self, conn) -> Dict[str, Dict]:
        """Get all pharmaceutical categories with their hierarchy."""
        with conn.cursor() as cur:
            cur.execute("""
                WITH RECURSIVE category_tree AS (
                    -- Base case: top-level categories
                    SELECT 
                        category_id,
                        name,
                        parent_category_id,
                        description,
                        1 as level,
                        ARRAY[name] as path
                    FROM pharmaceutical_categories
                    WHERE parent_category_id IS NULL
                    
                    UNION ALL
                    
                    -- Recursive case: child categories
                    SELECT 
                        c.category_id,
                        c.name,
                        c.parent_category_id,
                        c.description,
                        ct.level + 1,
                        ct.path || c.name
                    FROM pharmaceutical_categories c
                    JOIN category_tree ct ON c.parent_category_id = ct.category_id
                )
                SELECT 
                    category_id,
                    name,
                    parent_category_id,
                    description,
                    level,
                    path
                FROM category_tree
                ORDER BY path;
            """)

            categories = {}
            for row in cur.fetchall():
                categories[row[0]] = {
                    "name": row[1],
                    "parent_id": row[2],
                    "description": row[3],
                    "level": row[4],
                    "path": row[5],
                }

            return categories

    def get_uncategorized_ingredients(
        self, conn, sample_size: int = None
    ) -> List[Dict]:
        """
        Get ingredients that haven't been categorized yet.

        Args:
            conn: Database connection
            sample_size: If provided, limits the number of ingredients returned
        """
        with conn.cursor() as cur:
            if sample_size:
                # Get a random sample
                cur.execute(
                    """
                    SELECT 
                        i.ingredient_id,
                        i.name
                    FROM active_ingredients i
                    LEFT JOIN pharmaceutical_category_relations r 
                        ON i.ingredient_id = r.ingredient_id
                    WHERE r.ingredient_id IS NULL
                    ORDER BY RANDOM()
                    LIMIT %s;
                """,
                    (sample_size,),
                )
            else:
                # Get all uncategorized ingredients
                cur.execute("""
                    SELECT 
                        i.ingredient_id,
                        i.name
                    FROM active_ingredients i
                    LEFT JOIN pharmaceutical_category_relations r 
                        ON i.ingredient_id = r.ingredient_id
                    WHERE r.ingredient_id IS NULL
                    ORDER BY i.name;
                """)

            return [{"id": row[0], "name": row[1]} for row in cur.fetchall()]

    def create_categorization_prompt(
        self, ingredients: List[Dict], categories: Dict[str, Dict]
    ) -> str:
        """Create a prompt for the Gemini API to categorize ingredients."""
        # Create a hierarchical representation of categories
        category_desc = []
        for cat_id, cat_info in categories.items():
            indent = "  " * (cat_info["level"] - 1)
            desc = f"{cat_info['description']}" if cat_info["description"] else ""
            category_desc.append(f"{indent}- {cat_info['name']}: {desc}")

        category_text = "\n".join(category_desc)
        ingredient_names = [ing["name"] for ing in ingredients]
        names_list = '", "'.join(ingredient_names)

        return f'''You are a pharmaceutical database expert. Analyze these drug ingredients and categorize them according to the provided pharmaceutical categories. Return ONLY a JSON array.

Available Categories (hierarchical):
{category_text}

Input ingredients: "{names_list}"

Required JSON structure for each ingredient:
{{
    "original": "ingredient name exactly as provided",
    "categories": [
        {{
            "category_name": "exact category name from the list",
            "confidence": number between 0 and 1,
            "notes": "explanation of categorization"
        }}
    ]
}}

Categorization rules:
1. An ingredient can belong to multiple categories
2. Consider both direct categories and parent categories when relevant
3. Set confidence scores:
   - 0.95+ for definite matches
   - 0.85-0.94 for likely matches
   - 0.80-0.84 for possible matches
4. Include detailed notes explaining the categorization
5. Be thorough in analyzing each ingredient
6. Consider pharmaceutical classifications and mechanisms of action

IMPORTANT:
1. Response must be ONLY the JSON array
2. ALL strings must use double quotes
3. Category names must match EXACTLY
4. Include ALL input ingredients
5. Minimum confidence threshold is 0.80'''

    def process_batch(
        self, conn, batch: List[Dict], categories: Dict[str, Dict]
    ) -> Tuple[int, List[Dict]]:
        """Process a batch of ingredients and return suggested categorizations."""
        successful_mappings = []
        errors = 0

        try:
            # Create and send prompt
            prompt = self.create_categorization_prompt(batch, categories)
            self.logger.info(f"Processing batch of {len(batch)} ingredients")
            response, _ = gemini_api.generate_content(prompt)

            if not response:
                raise ValueError("Empty response from Gemini API")

            # Clean and parse response
            cleaned_response = response.strip()
            if "```json" in cleaned_response:
                cleaned_response = cleaned_response.split("```json", 1)[1]
            elif "```" in cleaned_response:
                cleaned_response = cleaned_response.split("```", 1)[1]
            if cleaned_response.endswith("```"):
                cleaned_response = cleaned_response.rsplit("```", 1)[0]

            # Remove any YAML-style markers
            if cleaned_response.startswith("---"):
                cleaned_response = cleaned_response.split("---", 1)[1]

            cleaned_response = cleaned_response.strip()
            self.logger.debug(f"Cleaned API response: {cleaned_response}")

            # Parse JSON response
            categorizations = json.loads(cleaned_response)

            # Validate and process categorizations
            valid_mappings = []
            for item in categorizations:
                if not all(key in item for key in ["original", "categories"]):
                    self.logger.warning(f"Invalid mapping structure: {item}")
                    continue

                # Find the ingredient ID
                ingredient = next(
                    (ing for ing in batch if ing["name"] == item["original"]), None
                )
                if not ingredient:
                    self.logger.warning(
                        f"Could not find ingredient: {item['original']}"
                    )
                    continue

                # Validate category mappings
                valid_categories = []
                for cat in item["categories"]:
                    if not all(
                        key in cat for key in ["category_name", "confidence", "notes"]
                    ):
                        self.logger.warning(f"Invalid category structure: {cat}")
                        continue

                    # Find category ID
                    category_id = next(
                        (
                            cid
                            for cid, cinfo in categories.items()
                            if cinfo["name"] == cat["category_name"]
                        ),
                        None,
                    )
                    if not category_id:
                        self.logger.warning(f"Unknown category: {cat['category_name']}")
                        continue

                    if cat["confidence"] < 0.85:
                        self.logger.info(
                            f"Skipping low confidence mapping: {item['original']} -> "
                            f"{cat['category_name']} (confidence: {cat['confidence']})"
                        )
                        continue

                    valid_categories.append(
                        {
                            "category_id": category_id,
                            "confidence": cat["confidence"],
                            "notes": cat["notes"],
                        }
                    )

                if valid_categories:
                    valid_mappings.append(
                        {
                            "ingredient_id": ingredient["id"],
                            "ingredient_name": ingredient["name"],
                            "categories": valid_categories,
                        }
                    )
                    self.logger.info(
                        f"Found {len(valid_categories)} valid categories for {ingredient['name']}"
                    )

                    # Log detailed categorization info to changes log
                    for cat in valid_categories:
                        category_name = categories[cat["category_id"]]["name"]
                        self.changes_logger.info(
                            f"Ingredient: {ingredient['name']}\n"
                            f"Category: {category_name}\n"
                            f"Confidence: {cat['confidence']}\n"
                            f"Notes: {cat['notes']}\n"
                            f"{'-' * 50}"
                        )

            successful_mappings.extend(valid_mappings)

        except Exception as e:
            self.logger.error(f"Error processing batch: {str(e)}")
            errors += 1

        return errors, successful_mappings

    def create_category_relations(self, conn, mappings: List[Dict]) -> Tuple[int, int]:
        """Create the category relations in the database."""
        successful = 0
        errors = 0

        try:
            with conn.cursor() as cur:
                for mapping in mappings:
                    for category in mapping["categories"]:
                        try:
                            cur.execute(
                                """
                                INSERT INTO pharmaceutical_category_relations
                                (ingredient_id, category_id, created_at)
                                VALUES (%s, %s, CURRENT_TIMESTAMP)
                                ON CONFLICT (ingredient_id, category_id) DO NOTHING
                                RETURNING ingredient_id;
                            """,
                                (mapping["ingredient_id"], category["category_id"]),
                            )

                            if cur.fetchone():
                                successful += 1
                                self.changes_logger.info(
                                    f"Created relation: {mapping['ingredient_name']} -> "
                                    f"{category['category_id']} "
                                    f"(confidence: {category['confidence']})"
                                )

                        except Exception as e:
                            self.logger.error(
                                f"Error creating relation for {mapping['ingredient_name']}: {e}"
                            )
                            errors += 1
                            continue

                conn.commit()

        except Exception as e:
            self.logger.error(f"Error in create_category_relations: {e}")
            conn.rollback()
            errors += 1

        return successful, errors

    def process_all_ingredients(self, sample_size: int = None):
        """Main processing function."""
        print(f"\nStarting ingredient processing (sample_size={sample_size})")
        start_time = datetime.now()

        try:
            # Initialize Gemini API
            print("Initializing Gemini API...")
            gemini_api.initialize_gemini()
            print("Gemini API initialized successfully")

            # Connect to database
            print("Connecting to database...")
            conn = psycopg2.connect(
                dbname=config.DB_NAME,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                host=config.DB_HOST,
            )
            print("Database connection established")

            try:
                # Get categories
                print("Fetching pharmaceutical categories...")
                categories = self.get_categories(conn)
                print(f"Found {len(categories)} pharmaceutical categories")

                # Get uncategorized ingredients
                print("Fetching uncategorized ingredients...")
                ingredients = self.get_uncategorized_ingredients(conn, sample_size)
                total_ingredients = len(ingredients)

                if sample_size:
                    print(f"Working with sample of {total_ingredients} ingredients")
                else:
                    print(f"Found {total_ingredients} uncategorized ingredients")

                if not ingredients:
                    print("No uncategorized ingredients found")
                    return

                # Process in batches
                all_mappings = []
                total_errors = 0

                # Log all ingredients in the sample
                print("\nIngredients to process:")
                for ingredient in ingredients:
                    print(f"- {ingredient['name']} (ID: {ingredient['id']})")

                for i in range(0, total_ingredients, self.batch_size):
                    batch = ingredients[i : i + self.batch_size]
                    batch_num = i // self.batch_size + 1
                    total_batches = (
                        total_ingredients + self.batch_size - 1
                    ) // self.batch_size

                    print(f"\nProcessing batch {batch_num}/{total_batches}")

                    # Rate limiting
                    time.sleep(4)  # Stay under 15 RPM

                    errors, mappings = self.process_batch(conn, batch, categories)
                    total_errors += errors
                    all_mappings.extend(mappings)

                    # Log progress
                    progress = (i + len(batch)) / total_ingredients * 100
                    print(
                        f"Progress: {progress:.1f}% - Found {len(mappings)} categorizations in this batch"
                    )

                # Show summary
                print("\nProcessing complete!")
                print(
                    f"Found {len(all_mappings)} potential categorizations across {total_ingredients} ingredients"
                )

                if len(all_mappings) > 0:
                    print("\nSample categorizations:")
                    for mapping in all_mappings[:5]:
                        print(f"\n{mapping['ingredient_name']}:")
                        for cat in mapping["categories"]:
                            print(
                                f"  - Category: {categories[cat['category_id']]['name']}"
                            )
                            print(f"    Confidence: {cat['confidence']}")
                            print(f"    Notes: {cat['notes']}")

                if sample_size:
                    print(
                        "\nThis was a test run. No changes will be made to the database."
                    )
                    print(
                        "Review the results and if they look good, run without --sample"
                    )
                else:
                    if (
                        input("\nCreate these category relations? (yes/no): ").lower()
                        == "yes"
                    ):
                        successful, errors = self.create_category_relations(
                            conn, all_mappings
                        )
                        processing_time = datetime.now() - start_time
                        print("\nResults:")
                        print(f"- Duration: {processing_time}")
                        print(f"- Successful relations: {successful}")
                        print(f"- Errors: {errors}")
                    else:
                        print("Operation cancelled")

            except Exception as e:
                print(f"ERROR during processing: {e}")
                traceback.print_exc()
                raise
            finally:
                conn.close()
                print("Database connection closed")

        except Exception as e:
            print(f"FATAL ERROR: {e}")
            traceback.print_exc()
            raise


def main():
    import argparse
    from datetime import datetime
    import sys
    import traceback

    try:
        print("Starting pharmaceutical category linker...")

        # Parse arguments
        parser = argparse.ArgumentParser(
            description="Link active ingredients to pharmaceutical categories."
        )
        parser.add_argument(
            "--batch-size", type=int, default=25, help="Batch size for processing"
        )
        parser.add_argument(
            "--sample", type=int, help="Number of ingredients to sample for testing"
        )
        args = parser.parse_args()

        print(f"Configuration: batch_size={args.batch_size}, sample_size={args.sample}")

        # Initialize linker
        print("Initializing category linker...")
        linker = PharmaceuticalCategoryLinker(batch_size=args.batch_size)

        # Process ingredients
        print("Starting ingredient processing...")
        start_time = datetime.now()
        linker.process_all_ingredients(sample_size=args.sample)
        duration = datetime.now() - start_time
        print(f"Processing completed in {duration}")

    except ImportError as e:
        print(f"ERROR: Failed to import required module: {e}")
        print(
            "Make sure you're running from the correct directory and all dependencies are installed"
        )
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Unexpected error occurred: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
