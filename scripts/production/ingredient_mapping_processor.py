import psycopg2
import re
import json
import time
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import gemini_api
import config
from utils.logger_setup import LoggerSetup


class IngredientMappingProcessor:
    def __init__(self, batch_size: int = 25):
        """Initialize the ingredient mapping processor with proper batching.
        
        Args:
            batch_size: Number of ingredients to process per batch (default: 25)
        """
        self.batch_size = batch_size
        
        # Initialize logging
        logger_setup = LoggerSetup(
            "IngredientMappingProcessor",
            log_dir="logs",
            extra_logger="mapping_details"
        )
        self.logger = logger_setup.get_logger()
        self.details_logger = logger_setup.get_extra_logger()
        
        self.logger.info(f"Ingredient Mapping Processor initialized with batch_size={batch_size}")

    def get_unprocessed_ingredients(self, conn, sample_size: int = None) -> List[Dict]:
        """Get ingredients that haven't been mapped yet."""
        with conn.cursor() as cur:
            if sample_size:
                cur.execute("""
                    SELECT ai.ingredient_id, ai.name 
                    FROM active_ingredients ai
                    LEFT JOIN ingredient_mappings im ON ai.ingredient_id = im.raw_ingredient_id
                    WHERE im.raw_ingredient_id IS NULL
                    ORDER BY RANDOM()
                    LIMIT %s;
                """, (sample_size,))
            else:
                cur.execute("""
                    SELECT ai.ingredient_id, ai.name 
                    FROM active_ingredients ai
                    LEFT JOIN ingredient_mappings im ON ai.ingredient_id = im.raw_ingredient_id
                    WHERE im.raw_ingredient_id IS NULL
                    ORDER BY ai.name;
                """)
            
            return [
                {"id": row[0], "name": row[1]}
                for row in cur.fetchall()
            ]

    def get_extended_ingredients(self, conn) -> Dict[str, Dict]:
        """Get all ingredients from active_ingredients_extended table."""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, ingredient_name, short_description, processing_status
                FROM active_ingredients_extended
                WHERE processing_status != 'duplicate'
                ORDER BY ingredient_name;
            """)
            
            extended_ingredients = {}
            for row in cur.fetchall():
                extended_ingredients[row[1].lower()] = {
                    "id": row[0],
                    "name": row[1],
                    "description": row[2],
                    "status": row[3]
                }
            
            return extended_ingredients

    def clean_ingredient_name(self, name: str) -> List[str]:
        """Clean and extract individual ingredients from a compound name."""
        if not name:
            return []
        
        # Convert to lowercase for processing
        cleaned = name.lower().strip()
        
        # Remove common suffixes and prefixes
        cleaned = re.sub(r'\s*\([^)]*\)', '', cleaned)  # Remove parentheses
        cleaned = re.sub(r'\d+\.?\d*\s*mg/?ml?', '', cleaned)  # Remove dosages
        cleaned = re.sub(r'\d+\.?\d*\s*%', '', cleaned)  # Remove percentages
        cleaned = re.sub(r'\b(extract|oil|base|grade|powder|liquid|solution|injection)\b', '', cleaned)
        
        # Split on common separators
        separators = ['-', '+', '/', '&', ',', ' and ', '.', '|', ';']
        ingredients = [cleaned]
        
        for sep in separators:
            new_ingredients = []
            for ing in ingredients:
                if sep in ing:
                    parts = [p.strip() for p in ing.split(sep) if p.strip()]
                    new_ingredients.extend(parts)
                else:
                    new_ingredients.append(ing)
            ingredients = new_ingredients
        
        # Clean individual parts and fix typos
        final_ingredients = []
        for ing in ingredients:
            ing = ing.strip()
            if len(ing) > 2 and not ing.isdigit():  # Filter out short/numeric entries
                ing = self.fix_common_typos(ing)
                final_ingredients.append(ing.title())
        
        return final_ingredients

    def fix_common_typos(self, ingredient: str) -> str:
        """Fix common typos in ingredient names."""
        typo_fixes = {
            'alamond': 'almond',
            'meganesium': 'magnesium',
            'drotavarine': 'drotaverine',
            'hyaluran': 'hyaluronic acid',
            'eugenia caryophyllus': 'clove',
            'thiocolchicoside-floctafenine': 'thiocolchicoside',
            'te extract': 'thyme extract',
            'pastillia': 'pastille',
            'glyceryl mon': 'glyceryl mono',
            'branch chained': 'branched-chain',
        }
        
        ingredient_lower = ingredient.lower()
        for typo, correct in typo_fixes.items():
            if typo in ingredient_lower:
                ingredient = ingredient_lower.replace(typo, correct)
        
        return ingredient

    def find_fuzzy_matches(self, ingredient: str, extended_ingredients: Dict) -> List[Dict]:
        """Find potential fuzzy matches in extended ingredients."""
        matches = []
        ingredient_lower = ingredient.lower()
        
        # Exact match
        if ingredient_lower in extended_ingredients:
            matches.append({
                "match_type": "exact",
                "confidence": 1.0,
                "extended_ingredient": extended_ingredients[ingredient_lower]
            })
            return matches
        
        # Partial matches
        for ext_name, ext_data in extended_ingredients.items():
            # Check if ingredient is contained in extended name
            if ingredient_lower in ext_name:
                confidence = len(ingredient_lower) / len(ext_name)
                matches.append({
                    "match_type": "contained",
                    "confidence": min(0.9, confidence + 0.1),
                    "extended_ingredient": ext_data
                })
            # Check if extended name is contained in ingredient
            elif ext_name in ingredient_lower:
                confidence = len(ext_name) / len(ingredient_lower)
                matches.append({
                    "match_type": "contains",
                    "confidence": min(0.8, confidence + 0.2),
                    "extended_ingredient": ext_data
                })
        
        # Sort by confidence and return top 3
        matches.sort(key=lambda x: x['confidence'], reverse=True)
        return matches[:3]

    def create_gemini_batch_prompt(self, ingredient_batch: List[Dict], 
                                 extended_ingredients: Dict) -> str:
        """Create a prompt for analyzing a batch of ingredients."""
        
        # Prepare batch data (limit to batch size)
        batch_text = []
        for ing in ingredient_batch:
            batch_text.append(f'"{ing["name"]}"')
        
        # Get relevant extended ingredients for context (sample)
        extended_examples = list(extended_ingredients.keys())[:100]
        extended_text = '", "'.join(extended_examples)
        
        return f'''You are a pharmaceutical ingredient quality expert. Analyze these {len(ingredient_batch)} ingredient names and map them to standardized ingredients. Return ONLY a JSON object.

🚨 **CRITICAL INSTRUCTIONS**: 
1. If an ingredient contains separators (-, +, /, &, "and"), you MUST split it into multiple components and create separate mappings for each component. DO NOT mark compound ingredients as "NO MAPPING" - split them first!
2. **NO FALSE MAPPINGS**: Only map ingredients you can confidently identify (>0.85 confidence). For unmappable ingredients, provide suggestions in quality_issues: "CREATE_NEW_INGREDIENT: [name] - [description]" or "POSSIBLE_MAPPING: [suggested_name]".

INGREDIENTS TO ANALYZE:
{chr(10).join(batch_text)}

REFERENCE STANDARDIZED INGREDIENTS (sample):
"{extended_text}"

Return ONLY this JSON structure:
{{
    "mappings": [
        {{
            "original": "exact original ingredient name",
            "suggested_mappings": [
                {{
                    "clean_name": "standardized ingredient name",
                    "match_type": "exact|fuzzy|compound_split|typo_fix",
                    "confidence": number between 0.8 and 1.0,
                    "notes": "explanation of mapping reasoning"
                }}
            ],
            "quality_issues": ["list of issues found"],
            "compound_split": ["if compound, list individual ingredients"]
        }}
    ]
}}

Analysis Rules:
1. **MANDATORY COMPOUND SPLITTING**: For ANY ingredient containing separators (-, +, /, &, and, spaces between distinct ingredients), you MUST split into individual components and create MULTIPLE mappings:
   
   **Examples that MUST be split:**
   - "menthol - crystals - tea free oil" → 3 mappings: menthol→Levomenthol, crystals→Menthol crystals, tea free oil→Tea tree
   - "zinc oxide - aloe vera extract - panthenol" → 3 mappings: zinc oxide→Zinc oxide, aloe vera→Aloe vera, panthenol→Panthenol
   - "vitamin c + iron" → 2 mappings: vitamin c→Vitamin C, iron→Iron
   
   **NEVER use "NO MAPPING" or "Needs Research" for compound ingredients - ALWAYS split them first**
   - If you cannot identify ALL components, map the ones you CAN identify
   - Each component gets its own mapping entry in suggested_mappings array
2. Fix common typos and suggest mapping (alamond→almond, billbery→bilberry, saccharine→saccharin, tea free→tea tree)
3. Remove dosage info (13.3mg, 0.9%, etc.) and map the base ingredient
4. Match against reference ingredients using fuzzy matching when exact match not found
5. For generic terms (vitamins b1→Vitamin B1, zinc→Zinc), map to the most common form
6. VITAMIN MAPPING PREFERENCE: Always prefer standard vitamin names over sources or chemical names:
   - "vit c", "vitamin c" → prefer "Vitamin C" over "Ascorbic acid" or "Acerola"
   - "vit a", "vitamin a" → prefer "Vitamin A" over "Retinol" 
   - "folic acid" → prefer "Folic acid" over "Quatrafolic"
   - "vitamin e" → prefer "Vitamin E" over "Tocopherol"
   - "niacin", "vitamin b3" → prefer "Vitamin B3" or "Niacin"
7. BE DECISIVE: If you identify the correct ingredient, map it with appropriate confidence
8. **NO FALSE MAPPINGS**: If you cannot find a confident match (>0.85 confidence), use empty suggested_mappings array and provide suggestions:
   - NEVER map unrelated ingredients to random database entries 
   - Examples of WRONG mappings to avoid: "iron bisglycinate" → "Acetate", "azelaic acid" → "Acetic acid"
   - Only map if the ingredient is truly the same or a direct synonym (e.g., paracetamol → acetaminophen)
   - In quality_issues field, suggest: "CREATE_NEW_INGREDIENT: [ingredient_name] - [description]" for valid pharmaceutical ingredients
   - Or suggest: "POSSIBLE_MAPPING: [suggested_name]" if you think there might be a similar ingredient in database
   - Better to have NO MAPPING with suggestions than incorrect mapping
9. ALWAYS suggest mappings when you identify the correct ingredient, even with typo fixes
10. CRITICAL: For compound ingredients, provide multiple mappings - one for each identifiable component

IMPORTANT:
- Response must be ONLY valid JSON
- Include ALL {len(ingredient_batch)} input ingredients
- Use exact names from reference list when possible
- Explain reasoning in notes field'''

    def process_batch(self, conn, batch: List[Dict], 
                     extended_ingredients: Dict) -> Tuple[int, int, List[Dict]]:
        """Process a batch of ingredients and return mappings."""
        successful_mappings = []
        errors = 0
        processed_count = 0
        
        try:
            # Rate limiting - 4 seconds between API calls (15 RPM limit)
            time.sleep(4)
            
            # Create and send prompt
            prompt = self.create_gemini_batch_prompt(batch, extended_ingredients)
            self.logger.info(f"Processing batch of {len(batch)} ingredients with AI")
            
            response, _ = gemini_api.generate_content(prompt)
            
            if not response:
                raise ValueError("Empty response from Gemini API")
            
            # Clean and parse response
            cleaned_response = self.clean_api_response(response)
            analysis = json.loads(cleaned_response)
            
            if not isinstance(analysis, dict) or 'mappings' not in analysis:
                raise ValueError("Invalid response structure - missing 'mappings' key")
            
            # Process each ingredient mapping
            for mapping in analysis['mappings']:
                try:
                    original_name = mapping['original']
                    
                    # Find the original ingredient in the batch
                    original_ingredient = next(
                        (ing for ing in batch if ing['name'] == original_name), None
                    )
                    
                    if not original_ingredient:
                        self.logger.warning(f"Could not find original ingredient: {original_name}")
                        continue
                    
                    # Log all analyzed ingredients (both accepted and rejected)
                    suggested_mappings = mapping.get('suggested_mappings', [])
                    
                    if not suggested_mappings:
                        # No mappings suggested
                        self.details_logger.info(
                            f"NO MAPPING: {original_name}\n"
                            f"Issues: {', '.join(mapping.get('quality_issues', ['No suitable matches found']))}\n"
                            f"Reason: AI found no confident matches\n"
                            f"{'-' * 50}"
                        )
                        continue
                    
                    # Process suggested mappings
                    for suggested in suggested_mappings:
                        clean_name = suggested['clean_name'].lower()
                        
                        if suggested['confidence'] >= 0.8:
                            # ACCEPTED MAPPING
                            if clean_name in extended_ingredients:
                                successful_mappings.append({
                                    'raw_ingredient_id': original_ingredient['id'],
                                    'raw_ingredient_name': original_name,
                                    'extended_ingredient_id': extended_ingredients[clean_name]['id'],
                                    'extended_ingredient_name': extended_ingredients[clean_name]['name'],
                                    'mapping_type': 'ai_suggested',
                                    'confidence': suggested['confidence'],
                                    'extraction_method': suggested['match_type'],
                                    'ai_notes': suggested['notes'],
                                    'original_text': original_name,
                                    'quality_issues': mapping.get('quality_issues', [])
                                })
                                
                                processed_count += 1
                                
                                # Log successful mapping
                                self.details_logger.info(
                                    f"ACCEPTED: {original_name} -> {extended_ingredients[clean_name]['name']}\n"
                                    f"Confidence: {suggested['confidence']}\n"
                                    f"Method: {suggested['match_type']}\n"
                                    f"Notes: {suggested['notes']}\n"
                                    f"Issues: {', '.join(mapping.get('quality_issues', []))}\n"
                                    f"{'-' * 50}"
                                )
                            else:
                                # Clean name not found in extended ingredients
                                self.details_logger.info(
                                    f"REJECTED: {original_name} -> {suggested['clean_name']}\n"
                                    f"Confidence: {suggested['confidence']}\n"
                                    f"Reason: '{suggested['clean_name']}' not found in extended ingredients database\n"
                                    f"Notes: {suggested['notes']}\n"
                                    f"{'-' * 50}"
                                )
                        else:
                            # REJECTED - Low confidence
                            self.details_logger.info(
                                f"REJECTED: {original_name} -> {suggested['clean_name']}\n"
                                f"Confidence: {suggested['confidence']} (below 0.8 threshold)\n"
                                f"Method: {suggested['match_type']}\n"
                                f"Notes: {suggested['notes']}\n"
                                f"Issues: {', '.join(mapping.get('quality_issues', []))}\n"
                                f"{'-' * 50}"
                            )
                    
                except Exception as e:
                    self.logger.error(f"Error processing mapping for {mapping.get('original', 'unknown')}: {e}")
                    errors += 1
                    continue
            
        except Exception as e:
            self.logger.error(f"Error processing batch: {e}")
            errors += 1
        
        self.logger.info(f"Batch processed: {processed_count} mappings created, {errors} errors")
        return processed_count, errors, successful_mappings

    def clean_api_response(self, response: str) -> str:
        """Clean and validate the API response."""
        try:
            # Remove markdown code blocks
            cleaned = response.strip()
            if '```json' in cleaned:
                cleaned = cleaned.split('```json', 1)[1]
            elif '```' in cleaned:
                cleaned = cleaned.split('```', 1)[1]
            if cleaned.endswith('```'):
                cleaned = cleaned.rsplit('```', 1)[0]
            
            # Remove YAML markers
            if cleaned.startswith('---'):
                cleaned = cleaned.split('---', 1)[1]
            
            return cleaned.strip()
            
        except Exception as e:
            self.logger.error(f"Error cleaning API response: {e}")
            raise

    def save_mappings_to_db(self, conn, mappings: List[Dict]) -> Tuple[int, int]:
        """Save mappings to the ingredient_mappings table."""
        successful = 0
        errors = 0
        
        try:
            with conn.cursor() as cur:
                for mapping in mappings:
                    try:
                        cur.execute("""
                            INSERT INTO ingredient_mappings (
                                raw_ingredient_id, extended_ingredient_id, mapping_type,
                                confidence, extraction_method, ai_notes, original_text,
                                created_by
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (raw_ingredient_id, extended_ingredient_id) DO NOTHING
                            RETURNING id;
                        """, (
                            mapping['raw_ingredient_id'],
                            mapping['extended_ingredient_id'],
                            mapping['mapping_type'],
                            mapping['confidence'],
                            mapping['extraction_method'],
                            mapping['ai_notes'],
                            mapping['original_text'],
                            'ai_processor'
                        ))
                        
                        if cur.fetchone():
                            successful += 1
                    
                    except Exception as e:
                        self.logger.error(f"Error saving mapping: {e}")
                        errors += 1
                        continue
                
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Error in save_mappings_to_db: {e}")
            conn.rollback()
            errors += len(mappings)
        
        return successful, errors

    def process_all_ingredients(self, sample_size: int = None):
        """Main processing function with proper batching."""
        start_time = datetime.now()
        self.logger.info("Starting ingredient mapping process")
        
        try:
            # Initialize Gemini API
            gemini_api.initialize_gemini()
            
            # Connect to database
            conn = psycopg2.connect(
                dbname=config.DB_NAME,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                host=config.DB_HOST
            )
            
            try:
                # Get extended ingredients (load once)
                self.logger.info("Loading extended ingredients...")
                extended_ingredients = self.get_extended_ingredients(conn)
                self.logger.info(f"Loaded {len(extended_ingredients)} extended ingredients")
                
                # Get unprocessed ingredients
                self.logger.info("Loading unprocessed ingredients...")
                unprocessed_ingredients = self.get_unprocessed_ingredients(conn, sample_size)
                total_ingredients = len(unprocessed_ingredients)
                
                if sample_size:
                    self.logger.info(f"Processing sample of {total_ingredients} ingredients")
                else:
                    self.logger.info(f"Processing {total_ingredients} unprocessed ingredients")
                
                if not unprocessed_ingredients:
                    self.logger.info("No unprocessed ingredients found")
                    return
                
                # Process in batches
                total_mappings = 0
                total_errors = 0
                
                for i in range(0, total_ingredients, self.batch_size):
                    batch = unprocessed_ingredients[i:i + self.batch_size]
                    batch_num = i // self.batch_size + 1
                    total_batches = (total_ingredients + self.batch_size - 1) // self.batch_size
                    
                    self.logger.info(f"Processing batch {batch_num}/{total_batches}")
                    
                    # Process batch
                    processed_count, errors, mappings = self.process_batch(
                        conn, batch, extended_ingredients
                    )
                    
                    # Save to database
                    if mappings:
                        saved, save_errors = self.save_mappings_to_db(conn, mappings)
                        total_mappings += saved
                        total_errors += save_errors + errors
                        
                        self.logger.info(f"Saved {saved} mappings to database")
                    
                    # Progress update
                    progress = (i + len(batch)) / total_ingredients * 100
                    self.logger.info(f"Progress: {progress:.1f}% - Total mappings created: {total_mappings}")
                
                # Final summary
                duration = datetime.now() - start_time
                self.logger.info(f"""
                Processing Complete!
                Duration: {duration}
                Total mappings created: {total_mappings}
                Total errors: {total_errors}
                Ingredients processed: {total_ingredients}
                Success rate: {(total_mappings/(total_mappings+total_errors)*100):.1f}%
                """)
                
                # Show database statistics
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM get_mapping_stats();")
                    stats = cur.fetchone()
                    if stats:
                        self.logger.info(f"Database stats: {stats}")
                
            finally:
                conn.close()
                self.logger.info("Database connection closed")
                
        except Exception as e:
            self.logger.error(f"Fatal error: {e}")
            raise


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Process ingredient mappings with AI analysis")
    parser.add_argument('--sample', type=int, help='Sample size for testing')
    parser.add_argument('--batch-size', type=int, default=25, help='Batch size for processing')
    parser.add_argument('--full', action='store_true', help='Process all unprocessed ingredients')
    args = parser.parse_args()
    
    try:
        sample_size = None if args.full else args.sample
        processor = IngredientMappingProcessor(batch_size=args.batch_size)
        processor.process_all_ingredients(sample_size)
        
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()