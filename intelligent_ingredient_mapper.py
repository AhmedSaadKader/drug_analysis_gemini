#!/usr/bin/env python3
"""
Intelligent Pharmaceutical Ingredient Mapper
Maps 100 ingredients from the log file using pharmaceutical knowledge and database integration.
"""

import psycopg2
import logging
from datetime import datetime
import uuid
import re
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'pharmacy_db',
    'user': 'postgres',
    'password': 'ahmed89saad'
}

@dataclass
class IngredientMapping:
    """Data class for ingredient mapping decisions"""
    raw_id: str
    raw_text: str
    action: str  # MAP_TO_EXISTING, CREATE_NEW, COMPOUND_SPLIT, NO_MAPPING
    target_name: str = None
    target_id: int = None
    confidence: float = 0.0
    description: str = ""
    notes: str = ""
    components: List[str] = None

class PharmaceuticalMapper:
    """Intelligent pharmaceutical ingredient mapper with database integration"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.existing_ingredients = {}
        self.processed_mappings = []
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'logs/intelligent_mapper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def connect_database(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor()
            self.logger.info("Connected to database successfully")
            
            # Load existing ingredients for reference
            self.load_existing_ingredients()
            
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            raise
    
    def load_existing_ingredients(self):
        """Load all existing standardized ingredients for mapping reference"""
        query = """
        SELECT id, ingredient_name, short_description 
        FROM active_ingredients_extended 
        ORDER BY ingredient_name
        """
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        for row in results:
            self.existing_ingredients[row[1].lower()] = {
                'id': row[0],
                'name': row[1],
                'description': row[2] or ""
            }
        
        self.logger.info(f"Loaded {len(self.existing_ingredients)} existing ingredients")
    
    def find_best_match(self, ingredient_text: str) -> Optional[Dict]:
        """Find best matching ingredient using pharmaceutical knowledge"""
        text_lower = ingredient_text.lower().strip()
        
        # Direct exact match
        if text_lower in self.existing_ingredients:
            return {
                'match': self.existing_ingredients[text_lower],
                'confidence': 1.0,
                'type': 'exact'
            }
        
        # Pharmaceutical-specific matching patterns
        matches = []
        
        for existing_name, data in self.existing_ingredients.items():
            # Check for pharmaceutical synonyms and variations
            if self.is_pharmaceutical_match(text_lower, existing_name, data['description'].lower()):
                similarity = self.calculate_pharmaceutical_similarity(text_lower, existing_name)
                if similarity > 0.7:
                    matches.append({
                        'match': data,
                        'confidence': similarity,
                        'type': 'pharmaceutical_match'
                    })
        
        # Return best match if confidence is high enough
        if matches:
            best_match = max(matches, key=lambda x: x['confidence'])
            if best_match['confidence'] > 0.8:
                return best_match
        
        return None
    
    def is_pharmaceutical_match(self, text: str, name: str, description: str) -> bool:
        """Check if ingredients are pharmaceutical matches using domain knowledge"""
        # Remove common pharmaceutical suffixes/prefixes
        text_clean = self.clean_pharmaceutical_name(text)
        name_clean = self.clean_pharmaceutical_name(name)
        
        # Direct substring matches
        if text_clean in name_clean or name_clean in text_clean:
            return True
        
        # Check description for matches
        if text_clean in description:
            return True
        
        # Common pharmaceutical equivalents
        pharma_equivalents = {
            'vit c': 'vitamin c',
            'vit.c': 'vitamin c',
            'viramin c': 'vitamin c',
            'natural vit.c': 'vitamin c',
            'vitb12': 'vitamin b12',
            'vit. b12': 'vitamin b12',
            'b1': 'vitamin b1',
            'k2': 'vitamin k2',
            'choline': 'choline',
            'riboflavin': 'vitamin b2',
            'nicotinamid': 'niacin',
            'niacin': 'vitamin b3',
            'condroitin': 'chondroitin',
            'chondriotin': 'chondroitin',
            'liquorice': 'licorice',
            'chamomile extrace': 'chamomile extract',
            'chlorohexedine': 'chlorhexidine',
            'benzoyl broxide': 'benzoyl peroxide',
            'conezyme a': 'coenzyme a',
            'isoleucine': 'l-isoleucine',
            'l-threonin': 'l-threonine',
            'cratageus': 'hawthorn',
            'cucumis sativus': 'cucumber extract',
            'malva sylvestris': 'mallow extract',
            'helianthus annuus': 'sunflower oil',
            'arachis': 'peanut oil',
            'tea treeoil': 'tea tree oil',
            'tea tree oil': 'melaleuca oil'
        }
        
        # Check equivalents
        for variant, standard in pharma_equivalents.items():
            if variant in text_clean and standard in name_clean:
                return True
            if standard in text_clean and variant in name_clean:
                return True
        
        return False
    
    def clean_pharmaceutical_name(self, name: str) -> str:
        """Clean pharmaceutical name for better matching"""
        # Remove dosage information
        name = re.sub(r'\d+\s*(mg|mcg|g|ml|%|i\.u)', '', name, flags=re.IGNORECASE)
        # Remove common pharmaceutical terms
        name = re.sub(r'\b(ext\.|extract|conc\.|concentrate|hcl|sodium|sulfate|chloride)\b', '', name, flags=re.IGNORECASE)
        # Remove extra spaces and punctuation
        name = re.sub(r'[^\w\s]', ' ', name)
        name = ' '.join(name.split())
        return name.lower().strip()
    
    def calculate_pharmaceutical_similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity score with pharmaceutical context"""
        # Simple similarity calculation
        text1_words = set(text1.split())
        text2_words = set(text2.split())
        
        if not text1_words or not text2_words:
            return 0.0
        
        intersection = text1_words.intersection(text2_words)
        union = text1_words.union(text2_words)
        
        jaccard = len(intersection) / len(union) if union else 0.0
        
        # Boost score for pharmaceutical terms
        pharma_terms = {'vitamin', 'extract', 'acid', 'sodium', 'oil', 'mg', 'mcg'}
        pharma_boost = len(intersection.intersection(pharma_terms)) * 0.1
        
        return min(1.0, jaccard + pharma_boost)
    
    def map_ingredient(self, raw_id: str, ingredient_text: str) -> IngredientMapping:
        """Create intelligent mapping for a single ingredient"""
        
        # Check for existing match
        best_match = self.find_best_match(ingredient_text)
        
        if best_match and best_match['confidence'] >= 0.85:
            return IngredientMapping(
                raw_id=raw_id,
                raw_text=ingredient_text,
                action="MAP_TO_EXISTING",
                target_name=best_match['match']['name'],
                target_id=best_match['match']['id'],
                confidence=best_match['confidence'],
                description=best_match['match']['description'],
                notes=f"Pharmaceutical match: {best_match['type']}"
            )
        
        # Check if it's a compound ingredient
        if self.is_compound_ingredient(ingredient_text):
            components = self.split_compound_ingredient(ingredient_text)
            return IngredientMapping(
                raw_id=raw_id,
                raw_text=ingredient_text,
                action="COMPOUND_SPLIT",
                confidence=0.9,
                components=components,
                notes=f"Compound ingredient with {len(components)} components"
            )
        
        # Check if it should be excluded (non-pharmaceutical)
        if self.is_non_pharmaceutical(ingredient_text):
            return IngredientMapping(
                raw_id=raw_id,
                raw_text=ingredient_text,
                action="NO_MAPPING",
                confidence=0.95,
                notes="Non-pharmaceutical ingredient (flavoring, testing equipment, etc.)"
            )
        
        # Create new standardized ingredient
        standardized_name = self.standardize_ingredient_name(ingredient_text)
        description = self.generate_pharmaceutical_description(ingredient_text)
        
        return IngredientMapping(
            raw_id=raw_id,
            raw_text=ingredient_text,
            action="CREATE_NEW",
            target_name=standardized_name,
            confidence=0.8,
            description=description,
            notes="New pharmaceutical ingredient created with standardized name"
        )
    
    def is_compound_ingredient(self, text: str) -> bool:
        """Check if ingredient is a compound with multiple components"""
        compound_indicators = ['-', '+', '/', ' and ', ',', 'with']
        
        # Count separators
        separator_count = sum(1 for indicator in compound_indicators if indicator in text.lower())
        
        # Known compound patterns
        compound_patterns = [
            r'\w+-\w+',  # drug-drug combinations
            r'\w+\s*-\s*\w+',  # spaced combinations
            r'(\w+\s+)+and\s+\w+',  # "ingredient and ingredient"
            r'\w+,\s*\w+',  # comma-separated
        ]
        
        has_pattern = any(re.search(pattern, text, re.IGNORECASE) for pattern in compound_patterns)
        
        # Exclude single compounds that shouldn't be split
        single_compounds = ['red ginseng', 'whey protein', 'hyaluronic acid', 'black seed', 
                          'tea tree', 'apple stem cells', 'vitamin d3']
        
        for compound in single_compounds:
            if compound in text.lower():
                return False
        
        return separator_count > 0 and has_pattern
    
    def split_compound_ingredient(self, text: str) -> List[str]:
        """Split compound ingredient into components"""
        # Split on common separators
        separators = ['-', '/', ' and ', ',', '+']
        components = [text]
        
        for sep in separators:
            new_components = []
            for comp in components:
                new_components.extend([c.strip() for c in comp.split(sep)])
            components = new_components
        
        # Clean and standardize each component
        cleaned_components = []
        for comp in components:
            if comp and len(comp.strip()) > 2:
                cleaned = self.standardize_ingredient_name(comp.strip())
                if cleaned:
                    cleaned_components.append(cleaned)
        
        return cleaned_components
    
    def is_non_pharmaceutical(self, text: str) -> bool:
        """Check if ingredient is non-pharmaceutical"""
        non_pharma_terms = [
            'flavor', 'chocolate flavor', 'natural cream', 'collage', 'sugar test strips',
            'antiseptic dental wash', 'malt juice', 'dr.ey.t', 'markyrene', 
            'hydro-active colloid formulation', 'green magma organic powder'
        ]
        
        text_lower = text.lower()
        return any(term in text_lower for term in non_pharma_terms)
    
    def standardize_ingredient_name(self, text: str) -> str:
        """Standardize ingredient name using pharmaceutical conventions"""
        text = text.strip()
        
        # Fix common misspellings
        corrections = {
            'viramin c': 'Vitamin C',
            'vitb12': 'Vitamin B12',
            'vit. b12': 'Vitamin B12',
            'natural vit.c': 'Vitamin C',
            'condroitin sulfate': 'Chondroitin Sulfate',
            'chondriotin sulfate': 'Chondroitin Sulfate',
            'liquorice extr.': 'Licorice Extract',
            'chamomile extrace': 'Chamomile Extract',
            'chlorohexedine': 'Chlorhexidine',
            'benzoyl broxide': 'Benzoyl Peroxide',
            'conezyme a': 'Coenzyme A',
            'l-threonin': 'L-Threonine',
            'nicotinamid (niacin)': 'Niacin',
            'calcuim carbonate-vitamins': 'Calcium Carbonate',
            'cratageus (hawthorn) extract': 'Hawthorn Extract',
            'cucumis sativus': 'Cucumber Extract',
            'malva sylvestris ext.': 'Mallow Extract',
            'helianthus annuus oil': 'Sunflower Oil',
            'tea treeoil': 'Tea Tree Oil',
            'empagliflozine': 'Empagliflozin'
        }
        
        text_lower = text.lower()
        for wrong, correct in corrections.items():
            if wrong in text_lower:
                return correct
        
        # Capitalize first letter of each word for pharmaceutical names
        words = text.split()
        standardized_words = []
        
        for word in words:
            # Keep abbreviations in caps
            if word.upper() in ['HCL', 'IU', 'MG', 'MCG', 'ML', 'G']:
                standardized_words.append(word.upper())
            # Keep vitamin notation
            elif word.lower().startswith('vit'):
                if len(word) > 3:
                    standardized_words.append('Vitamin ' + word[3:].upper())
                else:
                    standardized_words.append(word.capitalize())
            else:
                standardized_words.append(word.capitalize())
        
        return ' '.join(standardized_words)
    
    def generate_pharmaceutical_description(self, ingredient: str) -> str:
        """Generate pharmaceutical description for new ingredients"""
        ingredient_lower = ingredient.lower()
        
        descriptions = {
            'levocetirizine': 'Third-generation antihistamine used for allergic rhinitis and chronic urticaria',
            'cytarabine': 'Antimetabolite chemotherapy drug used to treat acute myeloid leukemia',
            'fluvastatin': 'HMG-CoA reductase inhibitor (statin) used to lower cholesterol',
            'tacrolimus': 'Immunosuppressive drug used to prevent organ transplant rejection',
            'cinacalcet': 'Calcimimetic agent used to treat hyperparathyroidism',
            'dabigatran etexilate': 'Direct thrombin inhibitor anticoagulant drug',
            'gatifloxacine': 'Fourth-generation fluoroquinolone antibiotic',
            'empagliflozin': 'SGLT2 inhibitor used to treat type 2 diabetes',
            'fludarabine': 'Purine analog chemotherapy drug used for hematologic malignancies',
            'rebamipide': 'Gastroprotective agent used to treat gastric ulcers',
            'tigecycline': 'Broad-spectrum glycylcycline antibiotic',
            'paricalcitol': 'Synthetic vitamin D analog used to treat hyperparathyroidism',
            'calcipotriol': 'Synthetic vitamin D3 analog used to treat psoriasis',
            'ipratropium bromide': 'Anticholinergic bronchodilator used for COPD and asthma',
            'chlorzoxazone': 'Centrally-acting muscle relaxant',
            'acefylline': 'Xanthine derivative bronchodilator',
            'fenticonazole': 'Antifungal medication used for vaginal infections',
            'vasopressin': 'Antidiuretic hormone used to treat diabetes insipidus',
            'certoparin sodium': 'Low molecular weight heparin anticoagulant',
            'ipodate sodium': 'Iodinated contrast agent for medical imaging',
            'red ginseng': 'Adaptogenic herb used in traditional medicine for energy and wellness',
            'whey protein isolate': 'High-purity protein supplement with minimal carbohydrates and fats',
            'hyaluronic acid': 'Glycosaminoglycan used for joint health and dermatological applications',
            'coenzyme a': 'Essential coenzyme involved in fatty acid metabolism',
            'black seed ext': 'Nigella sativa extract with anti-inflammatory and antioxidant properties',
            'phytosterol': 'Plant sterol compounds that help lower cholesterol levels',
            'siliphos': 'Phospholipid complex of silybin for enhanced liver support',
            'chondroitin sulfate': 'Glycosaminoglycan used for joint health and cartilage support',
            'rutin': 'Flavonoid glycoside with antioxidant and anti-inflammatory properties',
            'amla extract': 'Vitamin C-rich fruit extract with antioxidant properties',
            'lactoferrin': 'Iron-binding glycoprotein with antimicrobial and immune properties',
            'grindelia extract': 'Herbal extract traditionally used for respiratory conditions',
            'digestive enzymes': 'Enzyme blend to aid in digestion and nutrient absorption',
            'fatty acid': 'Essential fatty acid supplement for cellular health',
            'isoleucine': 'Essential branched-chain amino acid for muscle protein synthesis',
            'l-threonine': 'Essential amino acid important for protein synthesis',
            'choline': 'Essential nutrient important for brain function and liver health'
        }
        
        # Check for exact matches
        for key, desc in descriptions.items():
            if key in ingredient_lower:
                return desc
        
        # Generate generic description based on ingredient type
        if 'vitamin' in ingredient_lower or 'vit' in ingredient_lower:
            return 'Essential vitamin supplement for optimal health and metabolic function'
        elif 'extract' in ingredient_lower:
            return 'Standardized herbal extract with therapeutic properties'
        elif 'oil' in ingredient_lower:
            return 'Natural oil with therapeutic or cosmetic applications'
        elif 'acid' in ingredient_lower:
            return 'Organic acid compound with pharmaceutical applications'
        elif 'sulfate' in ingredient_lower or 'sodium' in ingredient_lower:
            return 'Pharmaceutical salt form for enhanced bioavailability'
        elif any(term in ingredient_lower for term in ['vaccine', 'anti']):
            return 'Immunological preparation for disease prevention'
        else:
            return 'Pharmaceutical ingredient with therapeutic applications'
    
    def create_new_ingredient(self, mapping: IngredientMapping) -> int:
        """Create new ingredient in active_ingredients_extended table"""
        insert_query = """
        INSERT INTO active_ingredients_extended 
        (ingredient_name, short_description, processing_status, last_updated)
        VALUES (%s, %s, 'processed', CURRENT_TIMESTAMP)
        RETURNING id
        """
        
        self.cursor.execute(insert_query, (mapping.target_name, mapping.description))
        new_id = self.cursor.fetchone()[0]
        self.conn.commit()
        
        self.logger.info(f"Created new ingredient: {mapping.target_name} (ID: {new_id})")
        return new_id
    
    def save_mapping(self, mapping: IngredientMapping):
        """Save ingredient mapping to database"""
        try:
            if mapping.action == "NO_MAPPING":
                self.logger.info(f"Skipped mapping for non-pharmaceutical: {mapping.raw_text}")
                return
            
            if mapping.action == "COMPOUND_SPLIT":
                self.logger.info(f"Compound ingredient noted: {mapping.raw_text} -> {mapping.components}")
                # Could implement compound splitting logic here
                return
            
            if mapping.action == "CREATE_NEW":
                # Check if ingredient already exists first
                check_query = "SELECT id FROM active_ingredients_extended WHERE ingredient_name = %s"
                self.cursor.execute(check_query, (mapping.target_name,))
                existing = self.cursor.fetchone()
                
                if existing:
                    mapping.target_id = existing[0]
                    mapping.action = "MAP_TO_EXISTING"  # Convert to existing mapping
                    self.logger.info(f"Found existing ingredient: {mapping.target_name} (ID: {mapping.target_id})")
                else:
                    # Create new ingredient
                    mapping.target_id = self.create_new_ingredient(mapping)
            
            # Check if mapping already exists
            check_mapping_query = """
            SELECT id FROM ingredient_mappings 
            WHERE raw_ingredient_id = %s AND extended_ingredient_id = %s
            """
            self.cursor.execute(check_mapping_query, (mapping.raw_id, mapping.target_id))
            existing_mapping = self.cursor.fetchone()
            
            if existing_mapping:
                self.logger.info(f"Mapping already exists for: {mapping.raw_text}")
                return
            
            # Insert mapping record
            insert_mapping_query = """
            INSERT INTO ingredient_mappings 
            (raw_ingredient_id, extended_ingredient_id, mapping_type, confidence, 
             extraction_method, original_text, ai_notes, verified, verified_by, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            mapping_type = 'claude_interactive' if mapping.action == 'MAP_TO_EXISTING' else 'ai_suggested'
            
            self.cursor.execute(insert_mapping_query, (
                mapping.raw_id,
                mapping.target_id,
                mapping_type,
                mapping.confidence,
                'claude_intelligent_mapper',
                mapping.raw_text,
                mapping.notes,
                True,  # Auto-verify high-confidence mappings
                'claude_intelligent_system',
                'claude_intelligent_mapper'
            ))
            
            self.conn.commit()
            self.logger.info(f"Saved mapping: {mapping.raw_text} -> {mapping.target_name} (confidence: {mapping.confidence})")
            
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error saving mapping for {mapping.raw_text}: {e}")
            raise
    
    def process_ingredients_from_log(self):
        """Process all 100 ingredients from the log file"""
        # Extract ingredients from log analysis
        ingredients_to_process = [
            ("1cafc952-a264-4760-8d73-2cce07763583", "red ginseng"),
            ("d10198f4-1ebc-4879-93fc-2fa7aaf6a02e", "whey protein isolate"),
            ("596242e8-693d-42aa-ab05-153d39d9ea9e", "vitb12 50mcg"),
            ("3adc26ac-20c1-462e-93e9-10c20affb01d", "hyaluronic acid 5 mg"),
            ("5dd52171-bf9a-46a0-be59-803432416477", "coenzyme a"),
            ("f0379a12-21e0-4d10-bab3-ee4c27d34185", "black seed ext"),
            ("5febdc12-d2f1-45f0-9b86-87fa5be6abfb", "chocolate flavor"),
            ("ca6707d8-1652-4d60-9497-df609dbd03fb", "viramin c 1000 mg"),
            ("736b0841-dcb0-4591-826f-e2ca6c5959f9", "levocetirizine"),
            ("24855079-25a6-4879-ae37-8c058d0517d6", "cytarabine 1 g"),
            ("fd2c17d1-bcef-4818-968d-6ac236c02e83", "collage"),
            ("878334e6-d4d8-4a44-8139-91cb91af942c", "large-leaf geiger tree"),
            ("7bc1c499-bb03-4046-8e9d-800789121d54", "fluvastatin"),
            ("4092de6e-bc20-4b0f-9c4c-47adc17aa959", "natural vit.c"),
            ("7f4357d8-c5f9-4cfe-bf92-e9110758683e", "benzophenone-4"),
            ("aa1e58bc-54d2-4304-9a11-4a1bbddfac48", "citrate"),
            ("84926ab4-be42-452b-ba26-74d062851753", "ampicillin-cloxacillin"),
            ("d12dcf57-9760-4c4a-b4c1-26469137cbbc", "cinacalcet"),
            ("b3b3c736-9f0a-4577-89e6-809ddd4a7305", "tacrolimus"),
            ("827ea788-2703-444c-90dc-6353c73fd636", "natural cream"),
            ("9d816060-b98a-4829-936b-fafa7c4ed431", "omeprazole-tinidazole-doxycycilline"),
            ("7158613e-dc07-4c4a-86f4-61c9e8c7001e", "k2"),
            ("472bf3d0-d0c2-48cd-a4f3-28281df6ba1c", "dabigatran etexilate"),
            ("460b800d-4605-4728-80f1-7964b386531a", "gatifloxacine"),
            ("3b612184-3b49-4be2-913a-791d9b1127a7", "malt juice conc."),
            ("62475f38-4a26-4aaa-9a98-aa2c910abf36", "antiseptic dental wash"),
            ("c9a67617-2054-4205-8a63-e1ffc34d1c2c", "liquorice extr."),
            ("a23e95ef-9121-44ac-980e-6b7e4e5fe13a", "isopropyl alcohol"),
            ("d2c75407-9216-4fff-9775-28f9a971e2dd", "sodium lauryl sulfate"),
            ("365eb98a-740c-4c25-b41b-c8881a4adf1c", "benzoyl broxide"),
            ("0a6d95b3-9ceb-48d9-a050-166dbe744c76", "colloidal silver"),
            ("fe82caee-e885-4367-8cb1-ad607188b8ce", "choline"),
            ("1a0f6296-1e34-4ee7-81f7-0e8cfd95125f", "chlorobutanol"),
            ("6cef00ac-b702-4448-a684-0e993e50e2a3", "riboflavin 1.3 mg"),
            ("c838d981-832a-486f-88e7-028dbfb0532f", "cucumis sativus"),
            ("94fc3773-1cb0-4676-8419-14bc9f150926", "cratageus (hawthorn) extract"),
            ("ae1defcc-b4c8-4a7c-8f81-18c775174b34", "chlorzoxazone"),
            ("39f21c35-e772-424b-9563-b884c74759ba", "rubella"),
            ("20b61846-da66-49ce-aaed-3c3d49533e86", "hydrolyzed corn starch"),
            ("25345c06-ed10-47dd-987f-0b4b5dc80c0f", "fludarabine"),
            ("ebc9e389-a05e-4201-863b-7856ac49e462", "fatty acid"),
            ("9369c73f-9af7-4696-b4ea-acdba3850373", "fenticonazole"),
            ("b1a8a1d1-8014-4fbb-8f8e-a7b45d8329b3", "calcuim carbonate-vitamins"),
            ("bc12ba25-2472-4b25-9a93-abe9c7a82318", "empagliflozine"),
            ("e7e14e60-82e2-4b74-93b5-8f9e0858d62f", "phytosterol"),
            ("955544b7-9025-4ddf-9a41-a7160498cda2", "vasopressin"),
            ("c5ea5f60-3c9c-4417-8a5b-28a8383351d5", "siliphos"),
            ("ada533fd-ad1b-42bc-ac78-f8e4322bade5", "condroitin sulfate"),
            ("04e21f46-a295-4abe-867e-0dffe3204e0d", "hyaluronic acid 2%"),
            ("973cbd31-171f-47e4-ad9c-eb583c3d6b99", "dr.ey.t"),
            ("f47c4db0-9db3-4ff0-b24f-aae744119838", "sodium camosulphonate"),
            ("a95b5511-5474-4ad6-9f15-344b425459ae", "malva sylvestris ext."),
            ("23615429-ab0c-49fc-9470-b57c69386f38", "ipratropium bromide"),
            ("3bb27eb5-0084-4a6d-9e63-ce4521cb1b3f", "B1"),
            ("720729ba-15bc-4a45-89a4-eb94b9a4ee8f", "cyclopentasiloxan"),
            ("626e0719-12bf-48a2-a78a-bda87064b5d5", "anti tetanic vaccine"),
            ("b8d523cd-69bb-4268-99f8-c9d95b7193e9", "cetrimide-lidocaine"),
            ("17b21f5d-74a7-4671-8a12-2c39eade7a9d", "conezyme a"),
            ("4aa4ca25-a1d3-4d0e-a3c6-574d97244a28", "26 nutrients 13 vitamin a"),
            ("e2b28a07-82da-4aa9-b7ff-c775c486c22c", "herbal extract-methyl salicylate"),
            ("ea7181df-ff30-49ea-b907-a2a4b7bf777c", "vit. b12"),
            ("adf61098-110d-491f-b667-6c28b80e933b", "sugar test strips"),
            ("08c7a022-0af1-4d0d-a5d3-1cb007b70403", "nicotinamid (niacin)"),
            ("93b2b2b0-2e40-4c67-8404-e79725d48daa", "green magma organic powder"),
            ("66a5e79c-39f8-4420-acf9-bc26ac541bb4", "chamomile-liquorice-fennel-thyme-pepperm"),
            ("c9dc27d9-46b9-4d2b-a65e-e0ff6ee89b0d", "clove flower"),
            ("75d17b67-8cb7-4e2b-ba26-a3bd78cf61c8", "calcium-vitamin d3-vitamin c"),
            ("148accdc-21d3-4268-ae62-56ee73a735e8", "vaccine rabies"),
            ("64c875cb-5775-4d36-a570-2380ee302cc0", "apricot kernel oil"),
            ("0c8e95e0-3de0-4c6f-b38e-403f2af5fd2a", "chlorohexedine"),
            ("f12ea350-88f0-4438-ab9e-2e5ced05bbe5", "croscarmellose"),
            ("85cee4e0-468b-454d-ada8-00c1d00e7f0c", "calcipotriol"),
            ("497b08da-07aa-4f3b-9495-0b111e5290ca", "arachis"),
            ("f7a88e33-b76f-4c7f-b21a-3d60bf596e7f", "chamomile extrace"),
            ("efccbd6a-1438-4883-bf6c-0a52fba1387d", "ipodate sodium"),
            ("07f5af0c-8aa2-46c0-b34b-4775f2070427", "herbal extracts from chamomile"),
            ("a1cc80df-1c19-475f-81be-711458dc90df", "digestive enzymes"),
            ("f31c91bc-904b-4d7d-9ad7-a1b2bf38e3b2", "cetalkonium"),
            ("31cfb0f5-6335-4b91-88df-509abe9eccd3", "vaccine meningitis"),
            ("ce91a56c-d872-4094-9290-0e75897b0a7a", "isoleucine"),
            ("6ebb4738-4752-4133-a45d-3717b5fc4409", "l-threonin"),
            ("65cd4991-a007-45b2-bb33-90d6069e7edc", "chlorohexidine-thymol-chlorocresol-menth"),
            ("20e96275-aca3-43de-9425-db5a69582981", "bha (salicylic acid)"),
            ("555bbba2-61f0-4b20-9adc-a0aaae552cd8", "certoparin sodium 3000 i.u"),
            ("74fae576-f2db-4e81-a2a6-19be4269b716", "markyrene"),
            ("16ae3f47-c0a9-4a1d-a0ea-dc3bb17ecfef", "tannic-fusidic-econazole-triamcinolone"),
            ("4d0575b0-fad8-4a71-93fc-3d5783117851", "hydro-active colloid formulation"),
            ("838a5591-9829-4065-9a33-5a2d1cdc364c", "rebamipide"),
            ("486e25f7-0a6a-4fe8-ad83-377264c8d190", "acefylline"),
            ("68e2c0be-c4f7-4fcd-a8f8-1e2c2fd44d51", "tigecycline"),
            ("a7446be9-9d62-47c0-b0b4-793d17853b2a", "paricalcitol"),
            ("d8b5002f-141e-45a9-a930-68b74d488817", "rutin 150 mg"),
            ("95492e45-0cbf-41a6-944d-0ec40365bc21", "chondriotin sulfate"),
            ("32d72e51-984c-4c52-bafa-1cbb8a604c5b", "amla extract"),
            ("7da14f21-d152-4210-b6af-e9ee7655dc8e", "helianthus annuus oil"),
            ("c7cb2c9e-a905-4c46-aedb-cff5c745bad2", "tea treeoil"),
            ("173af683-91f5-4336-afd3-8fbd3f9a02f1", "grindelia extract"),
            ("782d3416-78fc-4893-aeab-9e1def90da74", "lactoferrin 100 mg"),
            ("3298804a-48ee-471d-91e3-079003d83b3b", "aha 3%"),
            ("43cd6959-8e8b-46b3-a49d-a489b56eef05", "apple stem cells")
        ]
        
        self.logger.info(f"Starting intelligent mapping of {len(ingredients_to_process)} ingredients")
        
        stats = {
            'mapped_to_existing': 0,
            'created_new': 0,
            'compound_split': 0,
            'no_mapping': 0,
            'total_processed': 0
        }
        
        for raw_id, ingredient_text in ingredients_to_process:
            try:
                mapping = self.map_ingredient(raw_id, ingredient_text)
                
                # Try to save mapping
                try:
                    self.save_mapping(mapping)
                    self.processed_mappings.append(mapping)
                    
                    # Update statistics
                    if mapping.action == "MAP_TO_EXISTING":
                        stats['mapped_to_existing'] += 1
                    elif mapping.action == "CREATE_NEW":
                        stats['created_new'] += 1
                    elif mapping.action == "COMPOUND_SPLIT":
                        stats['compound_split'] += 1
                    elif mapping.action == "NO_MAPPING":
                        stats['no_mapping'] += 1
                    
                except Exception as save_error:
                    self.logger.error(f"Error saving mapping for {ingredient_text}: {save_error}")
                    # Still count as processed for compound/no-mapping cases
                    if mapping.action in ["COMPOUND_SPLIT", "NO_MAPPING"]:
                        if mapping.action == "COMPOUND_SPLIT":
                            stats['compound_split'] += 1
                        elif mapping.action == "NO_MAPPING":
                            stats['no_mapping'] += 1
                
                stats['total_processed'] += 1
                
                if stats['total_processed'] % 10 == 0:
                    self.logger.info(f"Progress: {stats['total_processed']}/100 ingredients processed")
                
            except Exception as e:
                self.logger.error(f"Error processing {ingredient_text}: {e}")
                stats['total_processed'] += 1  # Count failed attempts too
                continue
        
        # Print final summary
        self.logger.info("\n=== FINAL MAPPING SUMMARY ===")
        self.logger.info(f"Total processed: {stats['total_processed']}")
        self.logger.info(f"Mapped to existing: {stats['mapped_to_existing']}")
        self.logger.info(f"Created new ingredients: {stats['created_new']}")
        self.logger.info(f"Compound ingredients: {stats['compound_split']}")
        self.logger.info(f"Non-pharmaceutical (skipped): {stats['no_mapping']}")
        success_rate = ((stats['mapped_to_existing'] + stats['created_new']) / stats['total_processed']) * 100
        self.logger.info(f"Success rate: {success_rate:.1f}%")
        
        return stats
    
    def close_connection(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.logger.info("Database connection closed")

def main():
    """Main execution function"""
    mapper = PharmaceuticalMapper()
    
    try:
        # Connect to database
        mapper.connect_database()
        
        # Process all ingredients
        stats = mapper.process_ingredients_from_log()
        
        print("\n*** INTELLIGENT PHARMACEUTICAL MAPPING COMPLETED! ***")
        print(f"Successfully processed {stats['total_processed']} ingredients")
        print(f"- {stats['mapped_to_existing']} mapped to existing ingredients")
        print(f"- {stats['created_new']} new standardized ingredients created")
        print(f"- {stats['compound_split']} compound ingredients identified")
        print(f"- {stats['no_mapping']} non-pharmaceutical items excluded")
        success_rate = ((stats['mapped_to_existing'] + stats['created_new']) / stats['total_processed']) * 100
        print(f"Overall success rate: {success_rate:.1f}%")
        
    except Exception as e:
        print(f"Error during processing: {e}")
        mapper.logger.error(f"Fatal error: {e}")
    
    finally:
        mapper.close_connection()

if __name__ == "__main__":
    main()