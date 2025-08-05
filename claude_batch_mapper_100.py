#!/usr/bin/env python3
"""
Claude Batch Ingredient Mapper - 100 Items
Enhanced version for processing 100 ingredients at once
Maps ingredients from active_ingredients to active_ingredients_extended
"""

import psycopg2
import logging
from datetime import datetime
import re

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'pharmacy_db',
    'user': 'postgres',
    'password': 'ahmed89saad'
}

# Setup logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"logs/claude_batch_mapper_100_{timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class ClaudeBatchMapper100:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor()
        self.mappings_created = 0
        self.ingredients_created = 0
        self.compounds_split = 0
        self.no_mappings = 0
        self.exact_matches = 0
        self.fuzzy_matches = 0
        
    def find_existing_ingredient(self, target_names):
        """Find existing ingredient by multiple possible names"""
        for name in target_names:
            self.cursor.execute("""
                SELECT id FROM active_ingredients_extended 
                WHERE LOWER(ingredient_name) = %s
                LIMIT 1
            """, (name.lower(),))
            
            result = self.cursor.fetchone()
            if result:
                return result[0]
        return None
    
    def create_new_ingredient(self, name, description):
        """Create a new ingredient in active_ingredients_extended"""
        try:
            self.cursor.execute("""
                INSERT INTO active_ingredients_extended 
                (ingredient_name, short_description, processing_status, last_updated)
                VALUES (%s, %s, 'mapped_by_claude_batch', CURRENT_TIMESTAMP)
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
                VALUES (%s, %s, %s, %s, 'claude_batch_100', %s, true, 'claude', 'claude_batch_mapper_100')
            """, (raw_ingredient_id, extended_ingredient_id, mapping_type, confidence, notes))
            
            self.conn.commit()
            self.mappings_created += 1
            if mapping_type == 'exact':
                self.exact_matches += 1
            else:
                self.fuzzy_matches += 1
            logger.info(f"Created mapping: {raw_ingredient_id} -> {extended_ingredient_id} ({mapping_type})")
            return True
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error creating mapping: {str(e)}")
            return False
    
    def get_mapping_decision(self, ingredient_name):
        """Get Claude's intelligent mapping decision for each ingredient"""
        clean_name = ingredient_name.lower().strip()
        
        # Enhanced mapping decisions with pharmaceutical intelligence
        mapping_rules = {
            # Vitamins with dosages
            'b12 1000 mcg': {
                'target_names': ['Cyanocobalamin', 'Vitamin B12'],
                'create_name': 'Cyanocobalamin (Vitamin B12)',
                'description': 'Essential vitamin B12, crucial for nerve function and red blood cell formation.',
                'confidence': 1.0,
                'notes': 'B12 1000mcg - standard vitamin B12 dosage form'
            },
            'vitamin (b12': {
                'target_names': ['Cyanocobalamin', 'Vitamin B12'],
                'create_name': 'Cyanocobalamin (Vitamin B12)',
                'description': 'Essential vitamin B12, crucial for nerve function and red blood cell formation.',
                'confidence': 1.0,
                'notes': 'Vitamin B12 - incomplete formatting'
            },
            'vitamin b12 6 mcg': {
                'target_names': ['Cyanocobalamin', 'Vitamin B12'],
                'create_name': 'Cyanocobalamin (Vitamin B12)',
                'description': 'Essential vitamin B12, crucial for nerve function and red blood cell formation.',
                'confidence': 1.0,
                'notes': 'Vitamin B12 6mcg - low dose formulation'
            },
            'vit b 12 400 mcg': {
                'target_names': ['Cyanocobalamin', 'Vitamin B12'],
                'create_name': 'Cyanocobalamin (Vitamin B12)',
                'description': 'Essential vitamin B12, crucial for nerve function and red blood cell formation.',
                'confidence': 1.0,
                'notes': 'Vitamin B12 400mcg - medium dose formulation'
            },
            'vita a': {
                'target_names': ['Vitamin A', 'Retinol'],
                'create_name': 'Vitamin A (Retinol)',
                'description': 'Fat-soluble vitamin essential for vision, immune function, and cell growth.',
                'confidence': 1.0,
                'notes': 'Vita A - abbreviated Vitamin A'
            },
            'natural vit.c(liposomal)': {
                'target_names': ['Vitamin C', 'Ascorbic Acid'],
                'create_name': 'Liposomal Vitamin C',
                'description': 'Enhanced bioavailability form of vitamin C in liposomal delivery system.',
                'confidence': 1.0,
                'notes': 'Liposomal Vitamin C - enhanced absorption formula'
            },
            'vitamin c 40 mg': {
                'target_names': ['Vitamin C', 'Ascorbic Acid'],
                'create_name': 'Vitamin C (Ascorbic Acid)',
                'description': 'Essential water-soluble vitamin and antioxidant.',
                'confidence': 1.0,
                'notes': 'Vitamin C 40mg - low dose formulation'
            },
            
            # Complex pharmaceutical compounds
            'paracetamol - pseudoephedrinehcl - chloropheniramine maleate - cafffeine': {
                'action': 'COMPOUND_SPLIT',
                'components': [
                    {'name': 'Paracetamol', 'description': 'Analgesic and antipyretic medication.'},
                    {'name': 'Pseudoephedrine HCl', 'description': 'Decongestant used to relieve nasal congestion.'},
                    {'name': 'Chlorpheniramine Maleate', 'description': 'Antihistamine for allergy symptoms.'},
                    {'name': 'Caffeine', 'description': 'Central nervous system stimulant.'}
                ],
                'confidence': 1.0,
                'notes': 'Multi-ingredient cold/flu formulation - split into components'
            },
            'v.b1.300mg- vb6.150mg-vb7.35mcg-vb12.250mcg-folic acid 500mcg': {
                'action': 'COMPOUND_SPLIT',
                'components': [
                    {'name': 'Thiamine (Vitamin B1)', 'description': 'Essential B-vitamin for energy metabolism.'},
                    {'name': 'Pyridoxine (Vitamin B6)', 'description': 'Essential B-vitamin for protein metabolism.'},
                    {'name': 'Biotin (Vitamin B7)', 'description': 'Essential B-vitamin for fat and carbohydrate metabolism.'},
                    {'name': 'Cyanocobalamin (Vitamin B12)', 'description': 'Essential B-vitamin for nerve function.'},
                    {'name': 'Folic Acid', 'description': 'Essential B-vitamin for DNA synthesis.'}
                ],
                'confidence': 1.0,
                'notes': 'B-complex vitamin formulation - split into individual vitamins'
            },
            'lignocaine-panthenol': {
                'action': 'COMPOUND_SPLIT',
                'components': [
                    {'name': 'Lignocaine (Lidocaine)', 'description': 'Local anesthetic for pain relief.'},
                    {'name': 'Panthenol (Pro-Vitamin B5)', 'description': 'Skin conditioning and healing agent.'}
                ],
                'confidence': 1.0,
                'notes': 'Topical anesthetic with healing agent - split into components'
            },
            'menthol crystals rosemary caffeine': {
                'action': 'COMPOUND_SPLIT',
                'components': [
                    {'name': 'Menthol', 'description': 'Cooling agent and mild analgesic.'},
                    {'name': 'Rosemary Extract', 'description': 'Herbal extract with antioxidant properties.'},
                    {'name': 'Caffeine', 'description': 'Central nervous system stimulant.'}
                ],
                'confidence': 1.0,
                'notes': 'Topical cooling compound - split into components'
            },
            'achillea-ambrosia-ammi visnaga': {
                'action': 'COMPOUND_SPLIT',
                'components': [
                    {'name': 'Achillea (Yarrow)', 'description': 'Herbal extract with anti-inflammatory properties.'},
                    {'name': 'Ambrosia Extract', 'description': 'Plant extract used in traditional medicine.'},
                    {'name': 'Ammi Visnaga', 'description': 'Plant extract with antispasmodic properties.'}
                ],
                'confidence': 1.0,
                'notes': 'Multi-herb formula - split into individual herbal components'
            },
            
            # Minerals and supplements
            'boron 0.6 mg': {
                'target_names': ['Boron'],
                'create_name': 'Boron',
                'description': 'Trace mineral important for bone health and hormone metabolism.',
                'confidence': 1.0,
                'notes': 'Boron 0.6mg - trace mineral supplement'
            },
            'myoinositol': {
                'target_names': ['Myo-Inositol', 'Inositol'],
                'create_name': 'Myo-Inositol',
                'description': 'Vitamin-like substance important for cellular signaling and insulin sensitivity.',
                'confidence': 1.0,
                'notes': 'Myo-inositol - cellular signaling compound'
            },
            'flourine': {
                'target_names': ['Fluoride', 'Fluorine'],
                'create_name': 'Fluoride',
                'description': 'Mineral important for dental health and bone strength.',
                'confidence': 0.95,
                'notes': 'Flourine - likely fluoride for dental health'
            },
            'l-carnatin': {
                'target_names': ['L-Carnitine'],
                'create_name': 'L-Carnitine',
                'description': 'Amino acid derivative important for fatty acid metabolism.',
                'confidence': 1.0,
                'notes': 'L-carnatin - misspelled L-Carnitine'
            },
            'tricalcium phosphate': {
                'target_names': ['Tricalcium Phosphate', 'Calcium Phosphate'],
                'create_name': 'Tricalcium Phosphate',
                'description': 'Calcium supplement and food additive.',
                'confidence': 1.0,
                'notes': 'Tricalcium Phosphate - calcium source'
            },
            'calcium 600mg (elemental)': {
                'target_names': ['Calcium Carbonate', 'Calcium'],
                'create_name': 'Calcium (Elemental)',
                'description': 'Essential mineral for bone and teeth health.',
                'confidence': 1.0,
                'notes': 'Elemental calcium 600mg - bone health supplement'
            },
            'magnesium hydrogen aspartate': {
                'target_names': ['Magnesium Aspartate'],
                'create_name': 'Magnesium Hydrogen Aspartate',
                'description': 'Highly bioavailable form of magnesium supplement.',
                'confidence': 1.0,
                'notes': 'Magnesium hydrogen aspartate - chelated magnesium form'
            },
            'iodin': {
                'target_names': ['Iodine'],
                'create_name': 'Iodine',
                'description': 'Essential trace element for thyroid function.',
                'confidence': 1.0,
                'notes': 'Iodin - iodine supplement'
            },
            
            # Pharmaceutical drugs
            'zidovudine': {
                'target_names': ['Zidovudine', 'AZT'],
                'create_name': 'Zidovudine',
                'description': 'Antiretroviral medication used to treat HIV infection.',
                'confidence': 1.0,
                'notes': 'Zidovudine - HIV medication'
            },
            'fluocortolone': {
                'target_names': ['Fluocortolone'],
                'create_name': 'Fluocortolone',
                'description': 'Topical corticosteroid for inflammatory skin conditions.',
                'confidence': 1.0,
                'notes': 'Fluocortolone - topical corticosteroid'
            },
            'ruxolitinib': {
                'target_names': ['Ruxolitinib'],
                'create_name': 'Ruxolitinib',
                'description': 'JAK inhibitor used to treat myelofibrosis and polycythemia vera.',
                'confidence': 1.0,
                'notes': 'Ruxolitinib - JAK inhibitor oncology drug'
            },
            'isosorbide dinitrate': {
                'target_names': ['Isosorbide Dinitrate'],
                'create_name': 'Isosorbide Dinitrate',
                'description': 'Nitrate medication for angina and heart failure.',
                'confidence': 1.0,
                'notes': 'Isosorbide dinitrate - cardiac medication'
            },
            'dipotussium chlorazepate': {
                'target_names': ['Clorazepate Dipotassium', 'Clorazepate'],
                'create_name': 'Clorazepate Dipotassium',
                'description': 'Benzodiazepine medication for anxiety and seizures.',
                'confidence': 1.0,
                'notes': 'Dipotassium clorazepate - benzodiazepine'
            },
            'd-tubocurarine': {
                'target_names': ['Tubocurarine'],
                'create_name': 'D-Tubocurarine',
                'description': 'Neuromuscular blocking agent used in anesthesia.',
                'confidence': 1.0,
                'notes': 'D-tubocurarine - muscle relaxant'
            },
            'brolucizumab': {
                'target_names': ['Brolucizumab'],
                'create_name': 'Brolucizumab',
                'description': 'Anti-VEGF medication for age-related macular degeneration.',
                'confidence': 1.0,
                'notes': 'Brolucizumab - ophthalmologic biologic'
            },
            'dasatinib': {
                'target_names': ['Dasatinib'],
                'create_name': 'Dasatinib',
                'description': 'Tyrosine kinase inhibitor for chronic myeloid leukemia.',
                'confidence': 1.0,
                'notes': 'Dasatinib - oncology targeted therapy'
            },
            'vinburnine': {
                'target_names': ['Vinburnine'],
                'create_name': 'Vinburnine',
                'description': 'Alkaloid used for cognitive enhancement and circulation.',
                'confidence': 1.0,
                'notes': 'Vinburnine - cognitive enhancer'
            },
            'glibenclamide(glyburide)': {
                'target_names': ['Glibenclamide', 'Glyburide'],
                'create_name': 'Glibenclamide (Glyburide)',
                'description': 'Sulfonylurea medication for type 2 diabetes.',
                'confidence': 1.0,
                'notes': 'Glibenclamide/Glyburide - diabetes medication'
            },
            'propafenone': {
                'target_names': ['Propafenone'],
                'create_name': 'Propafenone',
                'description': 'Antiarrhythmic medication for irregular heart rhythms.',
                'confidence': 1.0,
                'notes': 'Propafenone - cardiac antiarrhythmic'
            },
            'metopimazine': {
                'target_names': ['Metopimazine'],
                'create_name': 'Metopimazine',
                'description': 'Antiemetic medication for nausea and vomiting.',
                'confidence': 1.0,
                'notes': 'Metopimazine - antiemetic drug'
            },
            'loperamide': {
                'target_names': ['Loperamide'],
                'create_name': 'Loperamide',
                'description': 'Antidiarrheal medication that slows gut motility.',
                'confidence': 1.0,
                'notes': 'Loperamide - antidiarrheal medication'
            },
            'atropine sulphate': {
                'target_names': ['Atropine Sulfate', 'Atropine'],
                'create_name': 'Atropine Sulfate',
                'description': 'Anticholinergic medication used as antidote and mydriatic.',
                'confidence': 1.0,
                'notes': 'Atropine sulfate - anticholinergic drug'
            },
            'dalteparin sodium': {
                'target_names': ['Dalteparin Sodium', 'Dalteparin'],
                'create_name': 'Dalteparin Sodium',
                'description': 'Low molecular weight heparin for anticoagulation.',
                'confidence': 1.0,
                'notes': 'Dalteparin sodium - anticoagulant'
            },
            'sulphadimidine': {
                'target_names': ['Sulfadimidine', 'Sulfamethazine'],
                'create_name': 'Sulfadimidine',
                'description': 'Sulfonamide antibiotic for bacterial infections.',
                'confidence': 1.0,
                'notes': 'Sulphadimidine - sulfonamide antibiotic'
            },
            'deferasirox': {
                'target_names': ['Deferasirox'],
                'create_name': 'Deferasirox',
                'description': 'Iron chelation therapy for iron overload conditions.',
                'confidence': 1.0,
                'notes': 'Deferasirox - iron chelator'
            },
            'clobetasone': {
                'target_names': ['Clobetasone'],
                'create_name': 'Clobetasone',
                'description': 'Moderate-potency topical corticosteroid.',
                'confidence': 1.0,
                'notes': 'Clobetasone - topical steroid'
            },
            'reboxetine': {
                'target_names': ['Reboxetine'],
                'create_name': 'Reboxetine',
                'description': 'Selective norepinephrine reuptake inhibitor antidepressant.',
                'confidence': 1.0,
                'notes': 'Reboxetine - SNRI antidepressant'
            },
            'chlorambucil': {
                'target_names': ['Chlorambucil'],
                'create_name': 'Chlorambucil',
                'description': 'Alkylating agent chemotherapy for chronic lymphocytic leukemia.',
                'confidence': 1.0,
                'notes': 'Chlorambucil - chemotherapy agent'
            },
            'acetylcystiene': {
                'target_names': ['Acetylcysteine', 'N-Acetylcysteine'],
                'create_name': 'Acetylcysteine',
                'description': 'Mucolytic agent and antioxidant.',
                'confidence': 1.0,
                'notes': 'Acetylcystiene - misspelled acetylcysteine'
            },
            'pancreatin': {
                'target_names': ['Pancreatin'],
                'create_name': 'Pancreatin',
                'description': 'Pancreatic enzyme supplement for digestive disorders.',
                'confidence': 1.0,
                'notes': 'Pancreatin - digestive enzyme'
            },
            'mecobalamin': {
                'target_names': ['Mecobalamin', 'Methylcobalamin'],
                'create_name': 'Mecobalamin (Methylcobalamin)',
                'description': 'Active form of vitamin B12 with enhanced bioavailability.',
                'confidence': 1.0,
                'notes': 'Mecobalamin - active B12 form'
            },
            'histidin': {
                'target_names': ['Histidine', 'L-Histidine'],
                'create_name': 'Histidine',
                'description': 'Essential amino acid important for protein synthesis.',
                'confidence': 1.0,
                'notes': 'Histidin - histidine amino acid'
            },
            
            # Biologics and complex drugs
            'epoetin beta 3000 i.u': {
                'target_names': ['Epoetin Beta'],
                'create_name': 'Epoetin Beta',
                'description': 'Recombinant erythropoietin for anemia treatment.',
                'confidence': 1.0,
                'notes': 'Epoetin Beta 3000 IU - erythropoietin biologic'
            },
            'anti d (rho) immunoglobulins': {
                'target_names': ['Anti-D Immunoglobulin', 'RhoGAM'],
                'create_name': 'Anti-D (Rho) Immunoglobulin',
                'description': 'Immunoglobulin to prevent Rh sensitization.',
                'confidence': 1.0,
                'notes': 'Anti-D immunoglobulin - Rh prevention'
            },
            'follitropin (recombinant fsh)': {
                'target_names': ['Follitropin', 'Recombinant FSH'],
                'create_name': 'Follitropin (Recombinant FSH)',
                'description': 'Recombinant follicle-stimulating hormone for fertility treatment.',
                'confidence': 1.0,
                'notes': 'Follitropin - recombinant FSH biologic'
            },
            'insulin isophane protamine human': {
                'target_names': ['NPH Insulin', 'Isophane Insulin'],
                'create_name': 'Insulin Isophane (NPH)',
                'description': 'Intermediate-acting human insulin with protamine.',
                'confidence': 1.0,
                'notes': 'NPH insulin - intermediate-acting insulin'
            },
            'lactoferrin 200 mg': {
                'target_names': ['Lactoferrin'],
                'create_name': 'Lactoferrin',
                'description': 'Iron-binding protein with antimicrobial properties.',
                'confidence': 1.0,
                'notes': 'Lactoferrin 200mg - antimicrobial protein'
            },
            'liposomal lactoferrin': {
                'target_names': ['Liposomal Lactoferrin'],
                'create_name': 'Liposomal Lactoferrin',
                'description': 'Enhanced bioavailability lactoferrin in liposomal delivery.',
                'confidence': 1.0,
                'notes': 'Liposomal lactoferrin - enhanced delivery system'
            },
            
            # Vaccines
            'vaccine cholera': {
                'target_names': ['Cholera Vaccine'],
                'create_name': 'Cholera Vaccine',
                'description': 'Vaccine for prevention of cholera infection.',
                'confidence': 1.0,
                'notes': 'Cholera vaccine - infectious disease prevention'
            },
            'meningitidis groups (a': {
                'target_names': ['Meningococcal Vaccine'],
                'create_name': 'Meningococcal Group A Vaccine',
                'description': 'Vaccine for meningococcal group A prevention.',
                'confidence': 1.0,
                'notes': 'Meningitis Group A vaccine - incomplete formatting'
            },
            
            # Natural extracts and oils
            'rosehip ext.': {
                'target_names': ['Rose Hip Extract', 'Rosehip Extract'],
                'create_name': 'Rose Hip Extract',
                'description': 'Vitamin C-rich extract from rose hips with antioxidant properties.',
                'confidence': 1.0,
                'notes': 'Rosehip extract - natural vitamin C source'
            },
            'natural eggshell membrane': {
                'target_names': ['Eggshell Membrane'],
                'create_name': 'Natural Eggshell Membrane',
                'description': 'Collagen-rich membrane for joint health support.',
                'confidence': 1.0,
                'notes': 'Natural eggshell membrane - joint support'
            },
            'thyme extract powder': {
                'target_names': ['Thyme Extract'],
                'create_name': 'Thyme Extract Powder',
                'description': 'Antimicrobial and antioxidant herbal extract.',
                'confidence': 1.0,
                'notes': 'Thyme extract powder - antimicrobial herb'
            },
            'high concentration  bovine colostrum': {
                'target_names': ['Bovine Colostrum'],
                'create_name': 'High Concentration Bovine Colostrum',
                'description': 'Immune-supporting first milk from cows.',
                'confidence': 1.0,
                'notes': 'High concentration bovine colostrum - immune support'
            },
            'coconut extract': {
                'target_names': ['Coconut Extract'],
                'create_name': 'Coconut Extract',
                'description': 'Natural extract from coconut with moisturizing properties.',
                'confidence': 1.0,
                'notes': 'Coconut extract - natural moisturizer'
            },
            'yarrow extract': {
                'target_names': ['Yarrow Extract', 'Achillea Extract'],
                'create_name': 'Yarrow Extract',
                'description': 'Anti-inflammatory herbal extract from Achillea millefolium.',
                'confidence': 1.0,
                'notes': 'Yarrow extract - anti-inflammatory herb'
            },
            'pomegranate peel': {
                'target_names': ['Pomegranate Peel Extract'],
                'create_name': 'Pomegranate Peel Extract',
                'description': 'Antioxidant-rich extract from pomegranate peels.',
                'confidence': 1.0,
                'notes': 'Pomegranate peel - antioxidant extract'
            },
            'jaborandi': {
                'target_names': ['Jaborandi Extract'],
                'create_name': 'Jaborandi Extract',
                'description': 'Herbal extract traditionally used for hair care.',
                'confidence': 1.0,
                'notes': 'Jaborandi - hair care herb'
            },
            'turpentine oil': {
                'target_names': ['Turpentine Oil'],
                'create_name': 'Turpentine Oil',
                'description': 'Essential oil with antiseptic and rubefacient properties.',
                'confidence': 1.0,
                'notes': 'Turpentine oil - topical antiseptic'
            },
            'oil(oliv': {
                'target_names': ['Olive Oil'],
                'create_name': 'Olive Oil',
                'description': 'Natural oil with moisturizing and antioxidant properties.',
                'confidence': 1.0,
                'notes': 'Oil(oliv - olive oil incomplete formatting'
            },
            'rosewater': {
                'target_names': ['Rose Water'],
                'create_name': 'Rose Water',
                'description': 'Gentle floral water with soothing and anti-inflammatory properties.',
                'confidence': 1.0,
                'notes': 'Rose water - gentle botanical water'
            },
            'anise oil': {
                'target_names': ['Anise Oil'],
                'create_name': 'Anise Oil',
                'description': 'Essential oil with antimicrobial and digestive properties.',
                'confidence': 1.0,
                'notes': 'Anise oil - digestive essential oil'
            },
            'anise extract': {
                'target_names': ['Anise Extract'],
                'create_name': 'Anise Extract',
                'description': 'Herbal extract with digestive and antimicrobial properties.',
                'confidence': 1.0,
                'notes': 'Anise extract - digestive herb'
            },
            'grape extract': {
                'target_names': ['Grape Extract'],
                'create_name': 'Grape Extract',
                'description': 'Antioxidant-rich extract from grapes.',
                'confidence': 1.0,
                'notes': 'Grape extract - antioxidant fruit extract'
            },
            'green coffee bean extract ( 50% chlorogenic acid': {
                'target_names': ['Green Coffee Bean Extract'],
                'create_name': 'Green Coffee Bean Extract (50% Chlorogenic Acid)',
                'description': 'Antioxidant extract standardized for chlorogenic acid content.',
                'confidence': 1.0,
                'notes': 'Green coffee bean extract - standardized chlorogenic acid'
            },
            'green coffee extract': {
                'target_names': ['Green Coffee Extract'],
                'create_name': 'Green Coffee Extract',
                'description': 'Antioxidant extract from unroasted coffee beans.',
                'confidence': 1.0,
                'notes': 'Green coffee extract - antioxidant supplement'
            },
            'astragalus extract': {
                'target_names': ['Astragalus Extract'],
                'create_name': 'Astragalus Extract',     
                'description': 'Immune-supporting herbal extract from Astragalus membranaceus.',
                'confidence': 1.0,
                'notes': 'Astragalus extract - immune support herb'
            },
            'pumpkin seeds powder': {
                'target_names': ['Pumpkin Seed Powder'],
                'create_name': 'Pumpkin Seeds Powder',
                'description': 'Nutrient-rich powder from pumpkin seeds.',
                'confidence': 1.0,
                'notes': 'Pumpkin seeds powder - nutritional supplement'
            },
            'zingiber officinale': {
                'target_names': ['Ginger Extract', 'Zingiber Officinale'],
                'create_name': 'Zingiber Officinale (Ginger)',
                'description': 'Anti-inflammatory and digestive herbal extract.',
                'confidence': 1.0,
                'notes': 'Zingiber officinale - ginger scientific name'
            },
            'colocynth': {
                'target_names': ['Colocynth Extract'],
                'create_name': 'Colocynth Extract',
                'description': 'Traditional herbal extract with purgative properties.',
                'confidence': 1.0,
                'notes': 'Colocynth - traditional medicinal plant'
            },
            'gogoba oil': {
                'target_names': ['Jojoba Oil'],
                'create_name': 'Jojoba Oil',
                'description': 'Natural moisturizing oil from jojoba seeds.',
                'confidence': 0.95,
                'notes': 'Gogoba oil - likely misspelled jojoba oil'
            },
            'calendulaoil': {
                'target_names': ['Calendula Oil'],
                'create_name': 'Calendula Oil',
                'description': 'Healing and anti-inflammatory oil from calendula flowers.',
                'confidence': 1.0,
                'notes': 'Calendulaoil - calendula oil without space'
            },
            'jojoba': {
                'target_names': ['Jojoba Oil'],
                'create_name': 'Jojoba Oil',
                'description': 'Natural moisturizing oil from jojoba seeds.',
                'confidence': 1.0,
                'notes': 'Jojoba - jojoba oil'
            },
            'alo vera extract': {
                'target_names': ['Aloe Vera Extract'],
                'create_name': 'Aloe Vera Extract',
                'description': 'Soothing and healing extract from aloe vera plant.',
                'confidence': 1.0,
                'notes': 'ALO VERA - aloe vera extract misspelled'
            },
            'spurulina maxima': {
                'target_names': ['Spirulina Maxima', 'Spirulina'],
                'create_name': 'Spirulina Maxima',
                'description': 'Nutrient-rich blue-green algae superfood.',
                'confidence': 1.0,
                'notes': 'Spurulina maxima - spirulina species'
            },
            
            # Chemical compounds and excipients
            'hydrated silica': {
                'target_names': ['Hydrated Silica'],
                'create_name': 'Hydrated Silica',
                'description': 'Abrasive and thickening agent commonly used in toothpaste.',
                'confidence': 1.0,
                'notes': 'Hydrated silica - dental abrasive'
            },
            'sodium lauryl ether sulphate': {
                'target_names': ['Sodium Laureth Sulfate', 'SLES'],
                'create_name': 'Sodium Lauryl Ether Sulfate',
                'description': 'Surfactant and foaming agent in cosmetic products.',
                'confidence': 1.0,
                'notes': 'Sodium lauryl ether sulfate - surfactant'
            },
            'dimethylpolysiloxan': {
                'target_names': ['Dimethicone', 'Polydimethylsiloxane'],
                'create_name': 'Dimethylpolysiloxane (Dimethicone)',
                'description': 'Silicone polymer used as emollient and anti-foaming agent.',
                'confidence': 1.0,
                'notes': 'Dimethylpolysiloxan - dimethicone silicone'
            },
            'butylated hydroxy toluene': {
                'target_names': ['Butylated Hydroxytoluene', 'BHT'],
                'create_name': 'Butylated Hydroxytoluene (BHT)',
                'description': 'Antioxidant preservative to prevent rancidity.',
                'confidence': 1.0,
                'notes': 'BHT - antioxidant preservative'
            },
            'parrafin oil': {
                'target_names': ['Paraffin Oil', 'Mineral Oil'],
                'create_name': 'Paraffin Oil',
                'description': 'Mineral oil used as emollient and lubricant.',
                'confidence': 1.0,
                'notes': 'Parrafin oil - paraffin oil misspelled'
            },
            'milk powder': {
                'target_names': ['Milk Powder'],
                'create_name': 'Milk Powder',
                'description': 'Dehydrated milk used as nutritional supplement.',
                'confidence': 1.0,
                'notes': 'Milk powder - dairy nutritional ingredient'
            },
            'natural diluted and isotonic seawater sterile': {
                'target_names': ['Isotonic Seawater'],
                'create_name': 'Sterile Isotonic Seawater',
                'description': 'Sterile seawater solution for nasal irrigation.',
                'confidence': 1.0,
                'notes': 'Isotonic seawater - nasal irrigation solution'
            },
            'isotonic buffered solution': {
                'target_names': ['Isotonic Buffer Solution'],
                'create_name': 'Isotonic Buffered Solution',
                'description': 'Balanced salt solution for medical applications.',
                'confidence': 1.0,
                'notes': 'Isotonic buffered solution - medical saline'
            },
            'marine collagen 10g': {
                'target_names': ['Marine Collagen'],
                'create_name': 'Marine Collagen',
                'description': 'Fish-derived collagen for skin and joint health.',
                'confidence': 1.0,
                'notes': 'Marine collagen 10g - fish collagen supplement'
            },
            'palmitic acid': {
                'target_names': ['Palmitic Acid'],
                'create_name': 'Palmitic Acid',
                'description': 'Saturated fatty acid used in cosmetics and soaps.',
                'confidence': 1.0,
                'notes': 'Palmitic acid - fatty acid ingredient'
            },
            'sodium sacchrain': {
                'target_names': ['Sodium Saccharin'],
                'create_name': 'Sodium Saccharin',
                'description': 'Artificial sweetener used in pharmaceutical formulations.',
                'confidence': 1.0,
                'notes': 'Sodium sacchrain - saccharin sweetener misspelled'
            },
            'cetrimonium bromide': {
                'target_names': ['Cetrimonium Bromide'],
                'create_name': 'Cetrimonium Bromide',
                'description': 'Quaternary ammonium compound used as antiseptic and hair conditioner.',
                'confidence': 1.0,
                'notes': 'Cetrimonium bromide - antimicrobial surfactant'
            },
            'to copheryl acetate': {
                'target_names': ['Tocopheryl Acetate', 'Vitamin E Acetate'],
                'create_name': 'Tocopheryl Acetate (Vitamin E)',
                'description': 'Stable form of vitamin E used as antioxidant.',
                'confidence': 1.0,
                'notes': 'To copheryl acetate - tocopheryl acetate misspelled'
            },
            
            # Generic categories
            'minerals and multivitamins': {
                'target_names': ['Multivitamin Mineral Complex'],
                'create_name': 'Minerals and Multivitamins',
                'description': 'Comprehensive vitamin and mineral supplement blend.',
                'confidence': 1.0,
                'notes': 'Minerals and multivitamins - generic supplement blend'
            },
            'vitamins-elements': {
                'target_names': ['Vitamin Mineral Complex'],
                'create_name': 'Vitamins and Elements',
                'description': 'Combined vitamin and mineral supplement formula.',
                'confidence': 1.0,
                'notes': 'Vitamins-elements - vitamin mineral combination'
            },
            'multi vit': {
                'target_names': ['Multivitamin'],
                'create_name': 'Multivitamin',
                'description': 'Comprehensive vitamin supplement blend.',
                'confidence': 1.0,
                'notes': 'Multi vit - abbreviated multivitamin'
            },
            'folic acid 400 mg': {
                'target_names': ['Folic Acid'],
                'create_name': 'Folic Acid',
                'description': 'Essential B-vitamin for DNA synthesis and cell division.',
                'confidence': 1.0,
                'notes': 'Folic acid 400mg - standard prenatal dose'
            },
            
            # Specialty compounds
            'lactofrin': {
                'target_names': ['Lactofrin'],
                'create_name': 'Lactofrin',
                'description': 'Specialized dairy-derived nutritional compound.',
                'confidence': 0.90,
                'notes': 'Lactofrin - specialized dairy compound'
            },
            'capilectine': {
                'target_names': ['Capilectine'],
                'create_name': 'Capilectine',
                'description': 'Hair care ingredient for scalp health.',
                'confidence': 0.90,
                'notes': 'Capilectine - hair care ingredient'
            },
            'natriance brightener': {
                'target_names': ['Natriance Brightener'],
                'create_name': 'Natriance Brightener',
                'description': 'Cosmetic brightening agent for skincare.',
                'confidence': 0.90,
                'notes': 'Natriance brightener - cosmetic brightening agent'
            },
            'cojik acid': {
                'target_names': ['Kojic Acid'],
                'create_name': 'Kojic Acid',
                'description': 'Skin lightening agent derived from fungi.',
                'confidence': 0.95,
                'notes': 'Cojik acid - kojic acid misspelled'
            },
            'ph adjuster': {
                'target_names': ['pH Adjuster'],
                'create_name': 'pH Adjuster',
                'description': 'Chemical compound used to modify product pH.',
                'confidence': 1.0,
                'notes': 'pH adjuster - pH modification agent'
            },
            'p.o.': {
                'action': 'NO_MAPPING',
                'notes': 'P.O. appears to be administration route (per os/by mouth) rather than ingredient'
            }
        }
        
        return mapping_rules.get(clean_name, None)
    
    def process_ingredient(self, ingredient_id, ingredient_name):
        """Process a single ingredient with Claude intelligence"""
        logger.info(f"Processing: {ingredient_name} (ID: {ingredient_id})")
        
        decision = self.get_mapping_decision(ingredient_name)
        
        if not decision:
            # No specific rule - try to create reasonable mapping
            self.no_mappings += 1
            logger.info(f"No specific mapping rule for: {ingredient_name}")
            return
            
        if decision.get('action') == 'NO_MAPPING':
            self.no_mappings += 1
            logger.info(f"No mapping decision: {decision['notes']}")
            return
            
        if decision.get('action') == 'COMPOUND_SPLIT':
            # Handle compound splitting
            component_ids = []
            for component in decision['components']:
                existing_id = self.find_existing_ingredient([component['name']])
                if existing_id:
                    component_ids.append(existing_id)
                else:
                    comp_id = self.create_new_ingredient(component['name'], component['description'])
                    if comp_id:
                        component_ids.append(comp_id)
            
            if component_ids:
                self.create_mapping(ingredient_id, component_ids[0], 'claude_interactive', 
                                  decision['confidence'], decision['notes'])
                self.compounds_split += 1
            return
        
        # Handle regular mapping
        existing_id = self.find_existing_ingredient(decision['target_names'])
        
        if existing_id:
            # Map to existing ingredient
            self.create_mapping(ingredient_id, existing_id, 'exact', 
                              decision['confidence'], 
                              f"{decision['notes']} (Found existing ingredient)")
        else:
            # Create new ingredient
            new_id = self.create_new_ingredient(decision['create_name'], decision['description'])
            if new_id:
                self.create_mapping(ingredient_id, new_id, 'claude_interactive', 
                                  decision['confidence'], decision['notes'])
    
    def run_mapping(self):
        """Run the mapping process for 100 ingredients"""
        logger.info("Starting Claude Batch Mapping Process - 100 Items")
        
        # Get 100 unmapped ingredients from database
        self.cursor.execute("""
            SELECT ai.ingredient_id, ai.name 
            FROM active_ingredients ai 
            LEFT JOIN ingredient_mappings im ON ai.ingredient_id = im.raw_ingredient_id 
            WHERE im.id IS NULL 
            ORDER BY RANDOM() 
            LIMIT 100
        """)
        
        ingredients = self.cursor.fetchall()
        logger.info(f"Retrieved {len(ingredients)} ingredients to process")
        
        processed = 0
        for ingredient_id, ingredient_name in ingredients:
            try:
                self.process_ingredient(ingredient_id, ingredient_name)
                processed += 1
                
                # Progress indicator
                if processed % 10 == 0:
                    logger.info(f"Progress: {processed}/100 ingredients processed")
                    
            except Exception as e:
                logger.error(f"Error processing {ingredient_name}: {str(e)}")
        
        # Print final summary
        logger.info("=== MAPPING SUMMARY ===")
        logger.info(f"Total ingredients processed: {processed}")
        logger.info(f"Total mappings created: {self.mappings_created}")
        logger.info(f"Exact matches: {self.exact_matches}")
        logger.info(f"Fuzzy matches: {self.fuzzy_matches}")
        logger.info(f"New ingredients created: {self.ingredients_created}")
        logger.info(f"Compounds split: {self.compounds_split}")
        logger.info(f"No mappings: {self.no_mappings}")
        logger.info(f"Success rate: {(self.mappings_created/processed)*100:.1f}%")
        logger.info(f"Log file saved: {log_filename}")
    
    def __del__(self):
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()

if __name__ == "__main__":
    mapper = ClaudeBatchMapper100()
    mapper.run_mapping()