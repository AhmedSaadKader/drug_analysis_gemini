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
import os
from datetime import datetime
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
              AND ai.name IS NOT NULL 
              AND TRIM(ai.name) != ''
              AND LENGTH(TRIM(ai.name)) > 0
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
              AND ai.name IS NOT NULL 
              AND TRIM(ai.name) != ''
              AND LENGTH(TRIM(ai.name)) > 0
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
        
    def create_mapping(self, conn, raw_id: str, extended_id: int, confidence: float = 1.0, method: str = "claude_interactive", original_text: str = "", ai_notes: str = "") -> None:
        """Create ingredient mapping."""
        cursor = conn.cursor()
        
        query = """
        INSERT INTO ingredient_mappings 
        (raw_ingredient_id, extended_ingredient_id, mapping_type, confidence, extraction_method, original_text, ai_notes, verified, verified_by, created_by)
        VALUES (%s, %s, 'claude_interactive', %s, %s, %s, %s, %s, %s, %s)
        """
        
        verified = confidence >= 1.0
        cursor.execute(query, (raw_id, extended_id, confidence, method, original_text, ai_notes, verified, 'claude_auto' if verified else None, 'claude_script'))
        conn.commit()
        cursor.close()
        
        self.logger.info(f"Created mapping: raw_id={raw_id} -> extended_id={extended_id} (confidence: {confidence})")

    def log_pending_decision(self, conn, ingredient: Dict, action: str, details: str, confidence: float, similar_ingredients: List[Dict]) -> None:
        """Log ingredients that need manual review."""
        cursor = conn.cursor()
        
        # Create a pending decisions table entry
        similar_json = json.dumps([{'name': s['name'], 'id': s['id'], 'description': s['description']} for s in similar_ingredients])
        
        query = """
        INSERT INTO claude_pending_decisions 
        (raw_ingredient_id, ingredient_name, suggested_action, suggested_details, confidence, similar_ingredients, created_at, status)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'pending')
        ON CONFLICT (raw_ingredient_id) DO UPDATE SET
            suggested_action = EXCLUDED.suggested_action,
            suggested_details = EXCLUDED.suggested_details,
            confidence = EXCLUDED.confidence,
            similar_ingredients = EXCLUDED.similar_ingredients,
            created_at = CURRENT_TIMESTAMP,
            status = 'pending'
        """
        
        try:
            cursor.execute(query, (ingredient['id'], ingredient['name'], action, details, confidence, similar_json))
            conn.commit()
            self.logger.info(f"Logged pending decision for: {ingredient['name']} (confidence: {confidence})")
        except psycopg2.Error as e:
            # Table might not exist, create it
            self.create_pending_decisions_table(conn)
            cursor.execute(query, (ingredient['id'], ingredient['name'], action, details, confidence, similar_json))
            conn.commit()
            self.logger.info(f"Logged pending decision for: {ingredient['name']} (confidence: {confidence})")
        finally:
            cursor.close()

    def create_pending_decisions_table(self, conn) -> None:
        """Create the pending decisions table if it doesn't exist."""
        cursor = conn.cursor()
        
        query = """
        CREATE TABLE IF NOT EXISTS claude_pending_decisions (
            id SERIAL PRIMARY KEY,
            raw_ingredient_id UUID NOT NULL,
            ingredient_name VARCHAR(255) NOT NULL,
            suggested_action VARCHAR(50) NOT NULL,
            suggested_details TEXT,
            confidence NUMERIC(3,2),
            similar_ingredients JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reviewed_at TIMESTAMP,
            reviewed_by VARCHAR(100),
            status VARCHAR(20) DEFAULT 'pending',
            final_decision TEXT,
            UNIQUE(raw_ingredient_id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_pending_decisions_status ON claude_pending_decisions(status);
        CREATE INDEX IF NOT EXISTS idx_pending_decisions_confidence ON claude_pending_decisions(confidence DESC);
        """
        
        cursor.execute(query)
        conn.commit()
        cursor.close()
        self.logger.info("Created claude_pending_decisions table")

    def find_extended_ingredient_by_name(self, conn, name: str) -> Optional[int]:
        """Find extended ingredient ID by name (case-insensitive exact match)."""
        cursor = conn.cursor()
        
        query = """
        SELECT id FROM active_ingredients_extended 
        WHERE LOWER(ingredient_name) = LOWER(%s)
        LIMIT 1
        """
        
        cursor.execute(query, (name.strip(),))
        result = cursor.fetchone()
        cursor.close()
        
        return result[0] if result else None

    def analyze_ingredient_automatically(self, conn, ingredient: Dict, similar: List[Dict]) -> Dict:
        """Automatically analyze ingredient and make mapping decision."""
        name = ingredient['name'].lower().strip()
        
        # Check for exact matches first
        for sim in similar:
            if sim['name'].lower().strip() == name:
                return {
                    'action': 'MAP_TO_EXISTING',
                    'details': sim['name'],
                    'confidence': 1.0,
                    'target_id': sim['id'],
                    'reasoning': f"Exact match found: '{name}' -> '{sim['name']}'"
                }
        
        # Check for very close matches (case variations, minor differences)
        for sim in similar:
            sim_name = sim['name'].lower().strip()
            if name in sim_name or sim_name in name:
                # Handle common variations
                if (name.replace('-', ' ') == sim_name.replace('-', ' ') or
                    name.replace(' ', '') == sim_name.replace(' ', '') or
                    abs(len(name) - len(sim_name)) <= 2):
                    return {
                        'action': 'MAP_TO_EXISTING',
                        'details': sim['name'],
                        'confidence': 0.95,
                        'target_id': sim['id'],
                        'reasoning': f"Close match found: '{name}' -> '{sim['name']}'"
                    }
        
        # Check if it's a known pharmaceutical ingredient that should be created
        pharma_keywords = ['tablet', 'capsule', 'mg', 'mcg', 'ml', 'ine', 'ide', 'ate', 'ole', 'ium']
        antibiotic_patterns = ['cef', 'pen', 'mycin', 'cillin', 'flox', 'zole']
        vitamin_patterns = ['vitamin', 'b1', 'b2', 'b3', 'b6', 'b12', 'c', 'd3', 'e', 'k']
        
        is_pharma = (any(keyword in name for keyword in pharma_keywords) or
                    any(pattern in name for pattern in antibiotic_patterns) or
                    any(pattern in name for pattern in vitamin_patterns) or
                    len(name) > 5)  # Reasonable length for ingredient names
        
        if is_pharma and name not in ['multivitamins', 'minerals', 'vitamins']:
            # Determine drug class and create description
            description = self.generate_ingredient_description(name)
            return {
                'action': 'CREATE_NEW',
                'details': description,
                'confidence': 0.95 if len(similar) == 0 else 0.85,
                'reasoning': f"Pharmaceutical ingredient not found in database: '{name}'"
            }
        
        # Handle compound ingredients
        if any(sep in name for sep in ['+', '/', ',', '-']) and len(name.split()) > 2:
            return {
                'action': 'COMPOUND_SPLIT',
                'details': f"Multi-component ingredient requiring individual mapping: {name}",
                'confidence': 0.8,
                'reasoning': f"Compound ingredient detected: '{name}'"
            }
        
        # Default to no mapping for unclear items
        return {
            'action': 'NO_MAPPING',
            'details': f"Unable to determine appropriate mapping for: {name}",
            'confidence': 0.6,
            'reasoning': f"Insufficient information to map: '{name}'"
        }
    
    def generate_ingredient_description(self, name: str) -> str:
        """Generate appropriate description for new ingredient."""
        name_lower = name.lower()
        
        # Antibiotics
        if any(pattern in name_lower for pattern in ['cef', 'pen', 'mycin', 'cillin']):
            return f"{name.title()} - Antibiotic medication used to treat bacterial infections"
        
        # Vitamins
        if 'vitamin' in name_lower or any(vit in name_lower for vit in ['b1', 'b2', 'b3', 'b6', 'b12']):
            return f"{name.title()} - Essential vitamin for proper body function and metabolism"
        
        # Oils and extracts
        if 'oil' in name_lower:
            return f"{name.title()} - Natural oil extract with therapeutic properties"
        if 'extract' in name_lower:
            return f"{name.title()} - Herbal extract with medicinal properties"
        
        # Hormones
        if any(term in name_lower for term in ['estradiol', 'testosterone', 'hormone']):
            return f"{name.title()} - Hormone used in medical treatments"
        
        # Anti-inflammatory
        if any(term in name_lower for term in ['mometasone', 'prednis', 'cortis']):
            return f"{name.title()} - Anti-inflammatory medication"
        
        # Default
        return f"{name.title()} - Pharmaceutical ingredient"

    def process_batch_automatically(self, conn, ingredients: List[Dict]) -> Dict:
        """Automatically process a batch of ingredients without external input."""
        print(f"\n{'='*60}")
        print("AUTOMATED BATCH PROCESSING")
        print('='*60)
        
        stats = {
            'processed': 0,
            'mapped': 0,
            'created': 0,
            'pending': 0,
            'skipped': 0,
            'details': []
        }
        
        for ingredient in ingredients:
            stats['processed'] += 1
            ingredient_name = ingredient['name']
            
            print(f"\nProcessing: {ingredient_name}")
            
            # Search for similar ingredients
            similar = self.search_similar_ingredients(conn, ingredient_name)
            
            # Automatically analyze and make decision
            decision = self.analyze_ingredient_automatically(conn, ingredient, similar)
            
            action = decision['action']
            details = decision['details']
            confidence = decision['confidence']
            reasoning = decision['reasoning']
            
            print(f"   Decision: {action}")
            print(f"   Details: {details}")
            print(f"   Confidence: {confidence}")
            print(f"   Reasoning: {reasoning}")
            
            # Record decision details
            decision_record = {
                'ingredient': ingredient_name,
                'action': action,
                'details': details,
                'confidence': confidence,
                'reasoning': reasoning,
                'similar_count': len(similar),
                'frequency': ingredient.get('frequency', 0)
            }
            stats['details'].append(decision_record)
            
            try:
                if action == 'NO_MAPPING':
                    print(f"   -> Skipped (no mapping needed)")
                    stats['skipped'] += 1
                    
                elif action == 'MAP_TO_EXISTING' and confidence >= 1.0:
                    # Auto-create mapping for perfect matches
                    target_id = decision.get('target_id')
                    if target_id:
                        self.create_mapping(
                            conn, ingredient['id'], target_id, confidence, 
                            'claude_auto_batch', ingredient_name, 
                            f"Auto-mapped: {reasoning}"
                        )
                        print(f"   SUCCESS: Auto-mapped to {details}")
                        stats['mapped'] += 1
                    else:
                        # Log for manual review
                        self.log_pending_decision(conn, ingredient, action, details, confidence, similar)
                        print(f"   PENDING: Logged for manual review (no target ID)")
                        stats['pending'] += 1
                        
                elif action == 'CREATE_NEW' and confidence >= 1.0:
                    # Auto-create new ingredient and mapping
                    new_id = self.add_new_ingredient(conn, ingredient_name, details)
                    self.create_mapping(
                        conn, ingredient['id'], new_id, confidence,
                        'claude_auto_batch', ingredient_name, 
                        f"Auto-created: {reasoning}"
                    )
                    print(f"   SUCCESS: Auto-created new ingredient")
                    stats['created'] += 1
                    stats['mapped'] += 1
                    
                else:
                    # Log for manual review (low confidence or complex cases)
                    self.log_pending_decision(conn, ingredient, action, details, confidence, similar)
                    print(f"   PENDING: Logged for manual review (confidence: {confidence})")
                    stats['pending'] += 1
                    
            except Exception as e:
                print(f"   ERROR: {e}")
                self.logger.error(f"Error processing {ingredient_name}: {e}")
                stats['skipped'] += 1
                decision_record['error'] = str(e)
        
        return stats

    def process_claude_responses(self, conn, ingredients: List[Dict]) -> Dict:
        """Process Claude responses and create mappings automatically."""
        print(f"\n{'='*60}")
        print("PROCESSING CLAUDE RESPONSES")
        print('='*60)
        
        stats = {
            'processed': 0,
            'mapped': 0,
            'created': 0,
            'pending': 0,
            'skipped': 0
        }
        
        print("Waiting for Claude responses...")
        print("Please provide responses in format:")
        print("INGREDIENT X: 'name'")
        print("Action: [MAP_TO_EXISTING | CREATE_NEW | NO_MAPPING | COMPOUND_SPLIT]")
        print("Details: [mapping target or description]")
        print("Confidence: [0.0-1.0] (optional, defaults based on action)")
        print("")
        
        responses = {}
        current_ingredient = None
        
        # Read responses from stdin
        try:
            while True:
                line = input().strip()
                if not line:
                    continue
                    
                if line.startswith("INGREDIENT"):
                    # Extract ingredient number and name
                    parts = line.split(":", 1)
                    if len(parts) == 2:
                        ingredient_part = parts[1].strip().strip("'\"")
                        current_ingredient = ingredient_part
                        responses[current_ingredient] = {}
                        
                elif line.startswith("Action:") and current_ingredient:
                    action = line.split(":", 1)[1].strip()
                    responses[current_ingredient]['action'] = action
                    
                elif line.startswith("Details:") and current_ingredient:
                    details = line.split(":", 1)[1].strip()
                    responses[current_ingredient]['details'] = details
                    
                elif line.startswith("Confidence:") and current_ingredient:
                    try:
                        confidence = float(line.split(":", 1)[1].strip())
                        responses[current_ingredient]['confidence'] = confidence
                    except ValueError:
                        pass
                        
        except EOFError:
            pass  # End of input
        
        # Process each ingredient
        for ingredient in ingredients:
            stats['processed'] += 1
            ingredient_name = ingredient['name']
            
            if ingredient_name not in responses:
                print(f"WARNING: No response found for: {ingredient_name}")  
                stats['skipped'] += 1
                continue
                
            response = responses[ingredient_name]
            action = response.get('action', '').upper()
            details = response.get('details', '')
            confidence = response.get('confidence')
            
            # Set default confidence based on action
            if confidence is None:
                if action == 'MAP_TO_EXISTING':
                    confidence = 1.0
                elif action == 'CREATE_NEW':
                    confidence = 0.95
                elif action == 'NO_MAPPING':
                    confidence = 1.0
                else:
                    confidence = 0.8
            
            print(f"\nProcessing: {ingredient_name}")
            print(f"   Action: {action}")
            print(f"   Details: {details}")
            print(f"   Confidence: {confidence}")
            
            try:
                if action == 'NO_MAPPING':
                    print(f"   -> Skipped (no mapping needed)")
                    stats['skipped'] += 1
                    
                elif action == 'MAP_TO_EXISTING':
                    # Find the target ingredient
                    target_id = self.find_extended_ingredient_by_name(conn, details)
                    
                    if target_id and confidence >= 1.0:
                        # Auto-create mapping for high confidence
                        self.create_mapping(
                            conn, ingredient['id'], target_id, confidence, 
                            'claude_auto', ingredient_name, 
                            f"Auto-mapped to {details} with confidence {confidence}"
                        )
                        print(f"   SUCCESS: Mapped to: {details} (ID: {target_id})")
                        stats['mapped'] += 1
                    else:
                        # Log for manual review
                        similar = self.search_similar_ingredients(conn, ingredient_name)
                        self.log_pending_decision(conn, ingredient, action, details, confidence, similar)
                        print(f"   PENDING: Logged for manual review")
                        stats['pending'] += 1
                        
                elif action == 'CREATE_NEW':
                    if confidence >= 1.0:
                        # Auto-create new ingredient and mapping
                        new_id = self.add_new_ingredient(conn, ingredient_name, details)
                        self.create_mapping(
                            conn, ingredient['id'], new_id, confidence,
                            'claude_auto', ingredient_name, 
                            f"Auto-created new ingredient: {details}"
                        )
                        print(f"   SUCCESS: Created new ingredient: {ingredient_name} (ID: {new_id})")
                        stats['created'] += 1
                        stats['mapped'] += 1
                    else:
                        # Log for manual review
                        similar = self.search_similar_ingredients(conn, ingredient_name)
                        self.log_pending_decision(conn, ingredient, action, details, confidence, similar)
                        print(f"   PENDING: Logged for manual review")
                        stats['pending'] += 1
                        
                else:
                    # Unknown action or COMPOUND_SPLIT - log for manual review
                    similar = self.search_similar_ingredients(conn, ingredient_name)
                    self.log_pending_decision(conn, ingredient, action, details, confidence, similar)
                    print(f"   PENDING: Logged for manual review")
                    stats['pending'] += 1
                    
            except Exception as e:
                print(f"   ERROR: Error processing {ingredient_name}: {e}")
                self.logger.error(f"Error processing {ingredient_name}: {e}")
                stats['skipped'] += 1
        
        return stats

def main():
    parser = argparse.ArgumentParser(description='Interactive Claude Ingredient Mapper')
    parser.add_argument('--sample', type=int, help='Number of ingredients to process (default: 20)')
    parser.add_argument('--batch', action='store_true', help='Process ingredients in continuous batches')
    parser.add_argument('--full', action='store_true', help='Process ALL unprocessed ingredients (use with caution)')
    parser.add_argument('--batch-size', type=int, default=20, help='Size of each batch when using --batch or --full')
    parser.add_argument('--auto', action='store_true', help='Fully automated processing - no user input required')
    parser.add_argument('--total', type=int, help='Total number of ingredients to process in automated mode')
    
    args = parser.parse_args()
    
    # Determine processing mode
    if args.auto:
        # Automated processing mode
        total_to_process = args.total or 100
        batch_size = args.batch_size
        
        print(f"AUTOMATED MODE: Processing {total_to_process} ingredients in batches of {batch_size}")
        print("This will run fully automatically with no user input required.")
        print("High confidence mappings will be created automatically.")
        print("Low confidence items will be logged for manual review.")
        
        mapper = ClaudeInteractiveMapper()
        conn = mapper.get_db_connection()
        if not conn:
            print("Failed to connect to database")
            return
            
        try:
            total_stats = {
                'processed': 0, 'mapped': 0, 'created': 0, 'pending': 0, 'skipped': 0,
                'all_details': []
            }
            
            batches_needed = (total_to_process + batch_size - 1) // batch_size
            
            for batch_num in range(1, batches_needed + 1):
                remaining = total_to_process - total_stats['processed']
                current_batch_size = min(batch_size, remaining)
                
                if current_batch_size <= 0:
                    break
                    
                print(f"\n{'='*80}")
                print(f"AUTOMATED BATCH {batch_num}/{batches_needed}")
                print(f"Processing {current_batch_size} ingredients...")
                print('='*80)
                
                # Get ingredients for this batch
                ingredients = mapper.get_unprocessed_ingredients(conn, current_batch_size)
                
                if not ingredients:
                    print("No more unprocessed ingredients found!")
                    break
                
                # Process batch automatically
                batch_stats = mapper.process_batch_automatically(conn, ingredients)
                
                # Update total stats
                for key in ['processed', 'mapped', 'created', 'pending', 'skipped']:
                    total_stats[key] += batch_stats[key]
                total_stats['all_details'].extend(batch_stats['details'])
                
                # Print batch results
                print(f"\n{'='*60}")
                print(f"BATCH {batch_num} COMPLETED")
                print('='*60)
                print(f"Processed: {batch_stats['processed']}")
                print(f"Auto-Mapped: {batch_stats['mapped']}")
                print(f"Auto-Created: {batch_stats['created']}")
                print(f"Pending Review: {batch_stats['pending']}")
                print(f"Skipped: {batch_stats['skipped']}")
                
                print(f"\nCUMULATIVE PROGRESS: {total_stats['processed']}/{total_to_process}")
            
            # Generate final report
            print(f"\n{'='*80}")
            print(f"AUTOMATED PROCESSING COMPLETE")
            print('='*80)
            print(f"Total Processed: {total_stats['processed']}")
            print(f"Total Auto-Mapped: {total_stats['mapped']}")
            print(f"Total Auto-Created: {total_stats['created']}")
            print(f"Total Pending Review: {total_stats['pending']}")
            print(f"Total Skipped: {total_stats['skipped']}")
            
            # Write detailed log file
            log_filename = f"logs/automated_mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            os.makedirs('logs', exist_ok=True)
            
            with open(log_filename, 'w') as f:
                json.dump({
                    'summary': total_stats,
                    'batch_details': total_stats['all_details'],
                    'timestamp': datetime.now().isoformat(),
                    'parameters': {
                        'total_requested': total_to_process,
                        'batch_size': batch_size,
                        'batches_processed': batch_num
                    }
                }, f, indent=2)
            
            print(f"\nDetailed log written to: {log_filename}")
            
            if total_stats['pending'] > 0:
                print(f"\nReview pending decisions with:")
                print(f"   SELECT * FROM claude_pending_decisions WHERE status = 'pending' ORDER BY confidence DESC;")
            
        finally:
            conn.close()
        return
        
    elif args.full:
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
                
                # Process Claude responses
                stats = mapper.process_claude_responses(conn, ingredients)
                
                # Print batch results
                print(f"\n{'='*60}")
                print(f"BATCH {batch_count} RESULTS")
                print('='*60)
                print(f"Processed: {stats['processed']}")
                print(f"Mapped: {stats['mapped']}")
                print(f"Created: {stats['created']}")
                print(f"Pending Review: {stats['pending']}")
                print(f"Skipped: {stats['skipped']}")
                
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
            
            # Process Claude responses
            stats = mapper.process_claude_responses(conn, ingredients)
            
            # Print results
            print(f"\n{'='*60}")
            print(f"FINAL RESULTS")
            print('='*60)
            print(f"Processed: {stats['processed']}")
            print(f"Mapped: {stats['mapped']}")
            print(f"Created: {stats['created']}")
            print(f"Pending Review: {stats['pending']}")
            print(f"Skipped: {stats['skipped']}")
            
            if stats['pending'] > 0:
                print(f"\nReview pending decisions with:")
                print(f"   SELECT * FROM claude_pending_decisions WHERE status = 'pending';")
            
            print(f"\nProcessing complete!")
        
    except Exception as e:
        mapper.logger.error(f"Error in interactive mapping: {e}")
        print(f"Error: {e}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()