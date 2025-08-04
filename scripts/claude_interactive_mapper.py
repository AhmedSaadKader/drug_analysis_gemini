#!/usr/bin/env python3
"""
Interactive Claude Ingredient Mapper
Presents ingredients to Claude for high-quality mapping in chat session
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
import argparse
import json
from typing import List, Dict, Optional
from utils.logger_setup import LoggerSetup
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME

class ClaudeInteractiveMapper:
    def __init__(self):
        """Initialize the interactive mapper."""
        self.logger_setup = LoggerSetup("ClaudeInteractiveMapper")
        self.logger = self.logger_setup.get_logger()
        
    def get_db_connection(self):
        """Create database connection."""
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                user=DB_USER, 
                password=DB_PASSWORD,
                database=DB_NAME
            )
            return conn
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            return None
            
    def get_unprocessed_ingredients(self, conn, limit: int = 20) -> List[Dict]:
        """Get unprocessed ingredients from active_ingredients table."""
        cursor = conn.cursor()
        
        # First check if we have a frequency column
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'active_ingredients'")
        columns = [row[0] for row in cursor.fetchall()]
        
        if 'frequency' in columns:
            query = """
            SELECT ai.ingredient_id, ai.name, 
                   COALESCE((SELECT COUNT(*) FROM product_ingredients pi WHERE pi.ingredient_id = ai.ingredient_id), 0) as frequency
            FROM active_ingredients ai
            LEFT JOIN ingredient_mappings im ON ai.ingredient_id = im.raw_ingredient_id
            WHERE im.raw_ingredient_id IS NULL
            ORDER BY frequency DESC
            LIMIT %s
            """
        else:
            query = """
            SELECT ai.ingredient_id, ai.name, 
                   COALESCE((SELECT COUNT(*) FROM product_ingredients pi WHERE pi.ingredient_id = ai.ingredient_id), 0) as frequency
            FROM active_ingredients ai
            LEFT JOIN ingredient_mappings im ON ai.ingredient_id = im.raw_ingredient_id
            WHERE im.raw_ingredient_id IS NULL
            ORDER BY frequency DESC
            LIMIT %s
            """
        
        cursor.execute(query, (limit,))
        results = cursor.fetchall()
        cursor.close()
        
        ingredients = []
        for row in results:
            ingredients.append({
                'id': row[0],
                'name': row[1], 
                'frequency': row[2]
            })
            
        return ingredients
        
    def search_similar_ingredients(self, conn, search_term: str, top_n: int = 10) -> List[Dict]:
        """Search for similar ingredients in extended database."""
        cursor = conn.cursor()
        
        # Search with multiple strategies
        search_lower = search_term.lower()
        
        query = """
        SELECT ingredient_name, short_description, id
        FROM active_ingredients_extended
        WHERE LOWER(ingredient_name) LIKE %s
           OR LOWER(ingredient_name) LIKE %s  
           OR LOWER(ingredient_name) LIKE %s
           OR LOWER(short_description) LIKE %s
        ORDER BY 
            CASE 
                WHEN LOWER(ingredient_name) = %s THEN 1
                WHEN LOWER(ingredient_name) LIKE %s THEN 2
                WHEN LOWER(ingredient_name) LIKE %s THEN 3
                ELSE 4
            END,
            LENGTH(ingredient_name)
        LIMIT %s
        """
        
        like_pattern = f'%{search_lower}%'
        starts_with = f'{search_lower}%'
        exact_match = search_lower
        
        cursor.execute(query, (
            exact_match, starts_with, like_pattern, like_pattern,
            exact_match, starts_with, like_pattern, top_n
        ))
        
        results = cursor.fetchall()
        cursor.close()
        
        similar = []
        for row in results:
            similar.append({
                'name': row[0],
                'description': row[1],
                'id': row[2]
            })
            
        return similar
        
    def present_ingredients_for_mapping(self, ingredients: List[Dict], conn) -> None:
        """Present ingredients to Claude for interactive mapping."""
        
        print("\n" + "="*80)
        print("CLAUDE INTERACTIVE INGREDIENT MAPPER")
        print("="*80)
        print(f"Processing {len(ingredients)} ingredients for mapping...")
        print("\nFor each ingredient, I'll show:")
        print("1. Original ingredient name & frequency") 
        print("2. Similar ingredients already in database")
        print("3. My mapping recommendation")
        print("\n" + "-"*80)
        
        for i, ingredient in enumerate(ingredients, 1):
            print(f"\nINGREDIENT {i}/{len(ingredients)}")
            print(f"Original: '{ingredient['name']}' (frequency: {ingredient['frequency']})")
            
            # Search for similar ingredients
            similar = self.search_similar_ingredients(conn, ingredient['name'])
            
            if similar:
                print(f"Similar ingredients found in database:")
                for j, sim in enumerate(similar[:5], 1):
                    desc = sim['description'][:60] + "..." if sim['description'] and len(sim['description']) > 60 else sim['description']
                    print(f"   {j}. {sim['name']} - {desc}")
            else:
                print("No similar ingredients found in database")
                
            print(f"\nCLAUDE'S RECOMMENDATION NEEDED:")
            print(f"   Ingredient: {ingredient['name']}")
            print(f"   Action: [MAP_TO_EXISTING | CREATE_NEW | NO_MAPPING | COMPOUND_SPLIT]")
            print(f"   Details: [If mapping, specify target. If new, provide description]")
            print("-"*80)
            
        print(f"\nREADY FOR CLAUDE ANALYSIS")
        print(f"Please analyze these {len(ingredients)} ingredients and provide mapping decisions!")
        
    def add_new_ingredient(self, conn, name: str, description: str, uses: str = "") -> int:
        """Add new ingredient to active_ingredients_extended."""
        cursor = conn.cursor() 
        
        query = """
        INSERT INTO active_ingredients_extended 
        (ingredient_name, short_description, common_uses, processing_status)
        VALUES (%s, %s, %s, 'completed')
        RETURNING id
        """
        
        cursor.execute(query, (name, description, uses))
        new_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        
        self.logger.info(f"Created new ingredient: {name} (ID: {new_id})")
        return new_id
        
    def create_mapping(self, conn, raw_id: int, extended_id: int, confidence: float = 1.0, method: str = "claude_interactive") -> None:
        """Create ingredient mapping."""
        cursor = conn.cursor()
        
        query = """
        INSERT INTO ingredient_mappings 
        (raw_ingredient_id, extended_ingredient_id, mapping_type, confidence, match_method, created_at)
        VALUES (%s, %s, 'claude_suggested', %s, %s, CURRENT_TIMESTAMP)
        """
        
        cursor.execute(query, (raw_id, extended_id, confidence, method))
        conn.commit()
        cursor.close()
        
        self.logger.info(f"Created mapping: raw_id={raw_id} -> extended_id={extended_id}")

def main():
    parser = argparse.ArgumentParser(description='Interactive Claude Ingredient Mapper')
    parser.add_argument('--sample', type=int, help='Number of ingredients to process (default: 20)')
    parser.add_argument('--batch', action='store_true', help='Process ingredients in continuous batches')
    parser.add_argument('--full', action='store_true', help='Process ALL unprocessed ingredients (use with caution)')
    parser.add_argument('--batch-size', type=int, default=20, help='Size of each batch when using --batch or --full')
    
    args = parser.parse_args()
    
    # Determine processing mode
    if args.full:
        sample_size = None  # Process all
        print("WARNING: --full mode will process ALL unprocessed ingredients.")
        print("This requires extensive Claude analysis time.")
        confirm = input("Continue? (y/N): ")
        if confirm.lower() != 'y':
            print("Cancelled.")
            return
    elif args.batch:
        sample_size = args.batch_size
        print(f"Batch mode: Processing {sample_size} ingredients per batch.")
        print("You'll be prompted to continue after each batch.")
    else:
        sample_size = args.sample or 20
    
    mapper = ClaudeInteractiveMapper()
    
    # Get database connection
    conn = mapper.get_db_connection()
    if not conn:
        print("Failed to connect to database")
        return
        
    try:
        if args.full or args.batch:
            # Batch processing mode
            batch_count = 1
            total_processed = 0
            
            while True:
                print(f"\n{'='*60}")
                print(f"BATCH {batch_count} - Processing {sample_size} ingredients")
                print('='*60)
                
                ingredients = mapper.get_unprocessed_ingredients(conn, sample_size)
                
                if not ingredients:
                    print("No more unprocessed ingredients found!")
                    print(f"Total processed: {total_processed} ingredients")
                    break
                    
                # Present to Claude for analysis
                mapper.present_ingredients_for_mapping(ingredients, conn)
                
                total_processed += len(ingredients)
                batch_count += 1
                
                if not args.full:  # In batch mode, ask to continue
                    print(f"\nBatch {batch_count-1} complete. Processed {len(ingredients)} ingredients.")
                    print(f"Total processed so far: {total_processed}")
                    continue_batch = input("Continue with next batch? (y/N): ")
                    if continue_batch.lower() != 'y':
                        print("Batch processing stopped.")
                        break
                        
        else:
            # Single sample mode
            ingredients = mapper.get_unprocessed_ingredients(conn, sample_size)
            
            if not ingredients:
                print("No unprocessed ingredients found!")
                return
                
            # Present to Claude for analysis
            mapper.present_ingredients_for_mapping(ingredients, conn)
        
    except Exception as e:
        mapper.logger.error(f"Error in interactive mapping: {e}")
        print(f"Error: {e}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()