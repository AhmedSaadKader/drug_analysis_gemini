#!/usr/bin/env python3
"""
Claude Batch Ingredient Mapper
Maps ingredients from active_ingredients to active_ingredients_extended
Creates mappings in ingredient_mappings table with detailed logging
"""

import psycopg2
import logging
from datetime import datetime
import re
import uuid

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'pharmacy_db',
    'user': 'postgres',
    'password': 'ahmed89saad'
}

# Setup logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"logs/claude_batch_mapper_{timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class ClaudeBatchMapper:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor()
        self.mappings_created = 0
        self.ingredients_created = 0
        self.compounds_split = 0
        self.no_mappings = 0
        
    def find_similar_ingredients(self, ingredient_name):
        """Find similar ingredients in active_ingredients_extended"""
        # Clean the ingredient name for searching
        clean_name = ingredient_name.lower().strip()
        
        # Search for exact matches first
        self.cursor.execute("""
            SELECT id, ingredient_name, short_description 
            FROM active_ingredients_extended 
            WHERE LOWER(ingredient_name) = %s
        """, (clean_name,))
        
        exact_matches = self.cursor.fetchall()
        if exact_matches:
            return exact_matches
        
        # Search for partial matches
        self.cursor.execute("""
            SELECT id, ingredient_name, short_description,
                   similarity(LOWER(ingredient_name), %s) as sim_score
            FROM active_ingredients_extended 
            WHERE similarity(LOWER(ingredient_name), %s) > 0.3
            ORDER BY sim_score DESC
            LIMIT 5
        """, (clean_name, clean_name))
        
        return self.cursor.fetchall()
    
    def create_new_ingredient(self, name, description):
        """Create a new ingredient in active_ingredients_extended"""
        try:
            self.cursor.execute("""
                INSERT INTO active_ingredients_extended 
                (ingredient_name, short_description, processing_status, last_updated)
                VALUES (%s, %s, 'mapped_by_claude', CURRENT_TIMESTAMP)
                RETURNING id
            """, (name, description))
            
            new_id = self.cursor.fetchone()[0]
            self.conn.commit()
            self.ingredients_created += 1
            logger.info(f"Created new ingredient: {name} (ID: {new_id})")
            return new_id
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error creating ingredient {name}: {str(e)}")
            return None
    
    def create_mapping(self, raw_ingredient_id, extended_ingredient_id, mapping_type, confidence, notes):
        """Create a mapping in ingredient_mappings table"""
        try:
            self.cursor.execute("""
                INSERT INTO ingredient_mappings 
                (raw_ingredient_id, extended_ingredient_id, mapping_type, confidence, 
                 extraction_method, ai_notes, verified, verified_by, created_by)
                VALUES (%s, %s, %s, %s, 'claude_batch', %s, true, 'claude', 'claude_batch_mapper')
            """, (raw_ingredient_id, extended_ingredient_id, mapping_type, confidence, notes))
            
            self.conn.commit()
            self.mappings_created += 1
            logger.info(f"Created mapping: {raw_ingredient_id} -> {extended_ingredient_id} ({mapping_type})")
            return True
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error creating mapping: {str(e)}")
            return False
    
    def process_ingredient(self, ingredient_id, ingredient_name):
        """Process a single ingredient and create appropriate mapping"""
        logger.info(f"Processing: {ingredient_name} (ID: {ingredient_id})")
        
        # Specific ingredient mappings based on pharmaceutical knowledge
        mapping_decisions = {
            'vitamins(c': {
                'action': 'MAP_TO_EXISTING',
                'target': 'Vitamin C',
                'confidence': 0.95,
                'notes': 'Vitamin C mapping - exact pharmaceutical match'
            },
            'bee propalis': {
                'action': 'CREATE_NEW',
                'target': 'Bee Propolis',
                'description': 'A resinous substance produced by bees, used for its antimicrobial and anti-inflammatory properties.',
                'confidence': 1.0,
                'notes': 'Standardized name for bee propolis - common natural ingredient'
            },
            'argireline': {
                'action': 'CREATE_NEW',
                'target': 'Argireline',
                'description': 'A synthetic peptide (acetyl hexapeptide-3) used in cosmetics to reduce wrinkles by inhibiting muscle contractions.',
                'confidence': 1.0,
                'notes': 'Well-known cosmetic peptide ingredient'
            },
            'melisse': {
                'action': 'CREATE_NEW',
                'target': 'Melissa (Lemon Balm)',
                'description': 'Melissa officinalis extract, used for its calming and antiviral properties.',
                'confidence': 1.0,
                'notes': 'Melissa/Melisse is lemon balm in French - herbal ingredient'
            },
            'clozapine': {
                'action': 'CREATE_NEW',
                'target': 'Clozapine',
                'description': 'An atypical antipsychotic medication used to treat treatment-resistant schizophrenia.',
                'confidence': 1.0,
                'notes': 'Important psychiatric medication - exact match'
            },
            'trandolapril': {
                'action': 'CREATE_NEW',
                'target': 'Trandolapril',
                'description': 'An ACE inhibitor used to treat high blood pressure and heart failure.',
                'confidence': 1.0,
                'notes': 'ACE inhibitor medication - exact pharmaceutical name'
            },
            'dill': {
                'action': 'CREATE_NEW',
                'target': 'Dill (Anethum graveolens)',
                'description': 'Dill herb extract used for its digestive and antimicrobial properties.',
                'confidence': 1.0,
                'notes': 'Common herbal ingredient with medicinal properties'
            },
            'vaccine pertussis-vacc tetanus toxoid': {
                'action': 'COMPOUND_SPLIT',
                'components': [
                    {'name': 'Pertussis Vaccine', 'description': 'Vaccine component against whooping cough (pertussis).'},
                    {'name': 'Tetanus Toxoid', 'description': 'Inactivated tetanus toxin used in tetanus vaccination.'}
                ],
                'confidence': 1.0,
                'notes': 'Combination vaccine components - should be split'
            },
            'b1(100mg)': {
                'action': 'MAP_TO_EXISTING',
                'target': 'Thiamine',
                'confidence': 0.95,
                'notes': 'B1 is thiamine (vitamin B1) - dosage notation ignored'
            },
            'cyclopent': {
                'action': 'CREATE_NEW',
                'target': 'Cyclopentolate',
                'description': 'A mydriatic agent used to dilate pupils for eye examinations.',
                'confidence': 0.90,
                'notes': 'Likely cyclopentolate based on context - ophthalmologic agent'
            },
            'sodium hyalornate': {
                'action': 'CREATE_NEW',
                'target': 'Sodium Hyaluronate',
                'description': 'The sodium salt of hyaluronic acid, used as a moisturizer and joint lubricant.',
                'confidence': 1.0,
                'notes': 'Common ingredient - corrected spelling from "hyalornate"'
            },
            'sun flower oil': {
                'action': 'CREATE_NEW',
                'target': 'Sunflower Oil',
                'description': 'Oil extracted from sunflower seeds, rich in vitamin E and used as an emollient.',
                'confidence': 1.0,
                'notes': 'Common cosmetic and pharmaceutical excipient'
            },
            'glucosamine sulphate': {
                'action': 'CREATE_NEW',
                'target': 'Glucosamine Sulfate',
                'description': 'A dietary supplement used to support joint health and cartilage maintenance.',
                'confidence': 1.0,
                'notes': 'Common joint health supplement - standardized spelling'
            },
            'irgazan(triclosan)': {
                'action': 'MAP_TO_EXISTING',
                'target': 'Triclosan',
                'confidence': 1.0,
                'notes': 'Irgazan is a brand name for triclosan - antimicrobial agent'
            },
            'licorice root extract': {
                'action': 'CREATE_NEW',
                'target': 'Licorice Root Extract',
                'description': 'Extract from Glycyrrhiza glabra root, used for its anti-inflammatory and soothing properties.',
                'confidence': 1.0,
                'notes': 'Common herbal extract in pharmaceuticals and cosmetics'
            },
            'dietary fibers': {
                'action': 'CREATE_NEW',
                'target': 'Dietary Fiber',
                'description': 'Indigestible carbohydrates that promote digestive health and regularity.',
                'confidence': 1.0,
                'notes': 'General category of nutritional supplement ingredient'
            },
            'glyceryl cocoate': {
                'action': 'CREATE_NEW',
                'target': 'Glyceryl Cocoate',
                'description': 'An emollient and emulsifier derived from coconut oil and glycerin.',
                'confidence': 1.0,
                'notes': 'Common cosmetic and pharmaceutical excipient'
            },
            'hydroxyurea(hydroxycarbamide)': {
                'action': 'CREATE_NEW',
                'target': 'Hydroxyurea',
                'description': 'An antineoplastic agent used to treat sickle cell disease and certain cancers.',
                'confidence': 1.0,
                'notes': 'Important medication - hydroxyurea and hydroxycarbamide are the same compound'
            },
            'fennel-caraway-peppermint-cardamon': {
                'action': 'COMPOUND_SPLIT',
                'components': [
                    {'name': 'Fennel Extract', 'description': 'Extract from Foeniculum vulgare, used for digestive support.'},
                    {'name': 'Caraway Extract', 'description': 'Extract from Carum carvi, used for digestive and antispasmodic effects.'},
                    {'name': 'Peppermint Extract', 'description': 'Extract from Mentha piperita, used for digestive and cooling effects.'},
                    {'name': 'Cardamom Extract', 'description': 'Extract from Elettaria cardamomum, used for digestive and aromatic properties.'}
                ],
                'confidence': 1.0,
                'notes': 'Multi-herb combination - should be split into individual components'
            },
            'vitamin d3 active form': {
                'action': 'CREATE_NEW',
                'target': 'Calcitriol (Active Vitamin D3)',
                'description': 'The active hormonal form of vitamin D3, used to regulate calcium and phosphate metabolism.',
                'confidence': 1.0,
                'notes': 'Active form of vitamin D3 is calcitriol - important hormone'
            }
        }
        
        clean_name = ingredient_name.lower().strip()
        
        if clean_name in mapping_decisions:
            decision = mapping_decisions[clean_name]
            
            if decision['action'] == 'MAP_TO_EXISTING':
                # Find the target ingredient
                self.cursor.execute("""
                    SELECT id FROM active_ingredients_extended 
                    WHERE LOWER(ingredient_name) LIKE %s
                    LIMIT 1
                """, (f"%{decision['target'].lower()}%",))
                
                result = self.cursor.fetchone()
                if result:
                    target_id = result[0]
                    self.create_mapping(ingredient_id, target_id, 'exact', 
                                      decision['confidence'], decision['notes'])
                else:
                    # Create new if target doesn't exist
                    new_id = self.create_new_ingredient(decision['target'], 
                                                      f"Pharmaceutical ingredient: {decision['target']}")
                    if new_id:
                        self.create_mapping(ingredient_id, new_id, 'claude_interactive', 
                                          decision['confidence'], decision['notes'])
            
            elif decision['action'] == 'CREATE_NEW':
                # First check if ingredient already exists
                self.cursor.execute("""
                    SELECT id FROM active_ingredients_extended 
                    WHERE LOWER(ingredient_name) = %s
                    LIMIT 1
                """, (decision['target'].lower(),))
                
                existing = self.cursor.fetchone()
                if existing:
                    target_id = existing[0]
                    self.create_mapping(ingredient_id, target_id, 'exact', 
                                      decision['confidence'], 
                                      f"{decision['notes']} (Found existing ingredient)")
                else:
                    new_id = self.create_new_ingredient(decision['target'], decision['description'])
                    if new_id:
                        self.create_mapping(ingredient_id, new_id, 'claude_interactive', 
                                          decision['confidence'], decision['notes'])
            
            elif decision['action'] == 'COMPOUND_SPLIT':
                # Create components and map to the first one
                component_ids = []
                for component in decision['components']:
                    # Check if component already exists
                    self.cursor.execute("""
                        SELECT id FROM active_ingredients_extended 
                        WHERE LOWER(ingredient_name) = %s
                        LIMIT 1
                    """, (component['name'].lower(),))
                    
                    existing = self.cursor.fetchone()
                    if existing:
                        component_ids.append(existing[0])
                    else:
                        comp_id = self.create_new_ingredient(component['name'], component['description'])
                        if comp_id:
                            component_ids.append(comp_id)
                
                if component_ids:
                    # Map to the first component as primary
                    self.create_mapping(ingredient_id, component_ids[0], 'claude_interactive', 
                                      decision['confidence'], decision['notes'])
                    self.compounds_split += 1
        
        else:
            # No specific mapping decision - log as no mapping
            logger.info(f"No specific mapping decision for: {ingredient_name}")
            self.no_mappings += 1
    
    def run_mapping(self):
        """Run the mapping process for 20 ingredients"""
        logger.info("Starting Claude Batch Mapping Process")
        
        # Get the 20 unmapped ingredients
        ingredients = [
            ('a4b3b8e8-4214-4f9e-84a9-db526dcdd683', 'vitamins(c'),
            ('52ed285b-7fca-42b0-93a3-70b69ec246cb', 'bee propalis'),
            ('6fca5d69-29ff-48bc-a9ed-ded2fc2dba24', 'argireline'),
            ('e36194e7-7d38-406b-ac77-9a599de25b83', 'melisse'),
            ('274540aa-1cc8-4fc0-af02-a100da2c6d21', 'clozapine'),
            ('f3c26ee2-c1d2-46ed-aa20-3dc76a25640a', 'trandolapril'),
            ('1b715d16-6984-4794-bc70-41216940d459', 'dill'),
            ('27415af7-304b-407f-a1ed-3b13dc02b5c5', 'vaccine pertussis-vacc tetanus toxoid'),
            ('7b422bab-7e15-45ba-a988-ab1986da1ea2', 'b1(100mg)'),
            ('848e162f-73e8-4f6a-8643-d221661ac5ed', 'cyclopent'),
            ('2246c458-66b2-46a5-870f-939731bedf09', 'sodium hyalornate'),
            ('c36d69b0-d761-4359-a318-14b541e55669', 'SUN FLOWER OIL'),
            ('174260df-78c0-4f7b-ba79-3567dde7dbce', 'glucosamine sulphate'),
            ('18364c5a-9be6-4824-8304-c5303193cd09', 'irgazan(triclosan)'),
            ('5cb9f410-08af-4f54-ae03-f318c6753ec4', 'licorice root extract'),
            ('3aec32cc-6080-49d3-9f56-141f095d43b8', 'dietary fibers'),
            ('1ed27486-7016-46dd-9703-4904b5521144', 'glyceryl cocoate'),
            ('57f92483-da09-41f9-98b4-69d0fc62dc8c', 'hydroxyurea(hydroxycarbamide)'),
            ('1f164729-713f-4317-a9aa-91c80b2de603', 'fennel-caraway-peppermint-cardamon'),
            ('d4fe7a42-6799-45a0-bc49-ee2ddcd2cd6e', 'vitamin d3 active form')
        ]
        
        for ingredient_id, ingredient_name in ingredients:
            try:
                self.process_ingredient(ingredient_id, ingredient_name)
            except Exception as e:
                logger.error(f"Error processing {ingredient_name}: {str(e)}")
        
        # Print summary
        logger.info("=== MAPPING SUMMARY ===")
        logger.info(f"Total mappings created: {self.mappings_created}")
        logger.info(f"New ingredients created: {self.ingredients_created}")
        logger.info(f"Compounds split: {self.compounds_split}")
        logger.info(f"No mappings: {self.no_mappings}")
        logger.info(f"Log file saved: {log_filename}")
    
    def __del__(self):
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()

if __name__ == "__main__":
    mapper = ClaudeBatchMapper()
    mapper.run_mapping()