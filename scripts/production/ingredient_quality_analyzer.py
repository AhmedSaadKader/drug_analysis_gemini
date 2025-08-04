import psycopg2
import re
import json
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import gemini_api
import config
from utils.logger_setup import LoggerSetup


class IngredientQualityAnalyzer:
    def __init__(self, sample_size: int = 100):
        """Initialize the ingredient quality analyzer.
        
        Args:
            sample_size: Number of ingredients to analyze in sample mode
        """
        self.sample_size = sample_size
        
        # Initialize logging
        logger_setup = LoggerSetup(
            "IngredientQualityAnalyzer",
            log_dir="logs",
            extra_logger="ingredient_analysis"
        )
        self.logger = logger_setup.get_logger()
        self.analysis_logger = logger_setup.get_extra_logger()
        
        self.logger.info("Ingredient Quality Analyzer initialized")

    def get_sample_ingredients(self, conn, limit: int = None) -> List[Dict]:
        """Get a sample of ingredients from active_ingredients table."""
        with conn.cursor() as cur:
            if limit:
                cur.execute("""
                    SELECT ingredient_id, name 
                    FROM active_ingredients 
                    ORDER BY RANDOM() 
                    LIMIT %s;
                """, (limit,))
            else:
                cur.execute("""
                    SELECT ingredient_id, name 
                    FROM active_ingredients 
                    ORDER BY name;
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
        cleaned = re.sub(r'\d+\s*mg/?ml?', '', cleaned)  # Remove dosages
        cleaned = re.sub(r'\d+\s*%', '', cleaned)  # Remove percentages
        cleaned = re.sub(r'\b(extract|oil|base|grade|powder|liquid)\b', '', cleaned)
        
        # Split on common separators
        separators = ['-', '+', '/', '&', ',', 'and', '.', '|']
        ingredients = [cleaned]
        
        for sep in separators:
            new_ingredients = []
            for ing in ingredients:
                if sep in ing:
                    parts = ing.split(sep)
                    new_ingredients.extend([p.strip() for p in parts if p.strip()])
                else:
                    new_ingredients.append(ing)
            ingredients = new_ingredients
        
        # Clean individual parts
        final_ingredients = []
        for ing in ingredients:
            ing = ing.strip()
            if len(ing) > 2 and not ing.isdigit():  # Filter out short/numeric entries
                # Fix common typos
                ing = self.fix_common_typos(ing)
                final_ingredients.append(ing)
        
        return final_ingredients

    def fix_common_typos(self, ingredient: str) -> str:
        """Fix common typos in ingredient names."""
        typo_fixes = {
            'alamond': 'almond',
            'meganesium': 'magnesium',
            'drotavarine': 'drotaverine',
            'hyaluran': 'hyaluronic acid',
            'eugenia caryophyllus': 'clove',
            'alfacalcidol': 'alfacalcidol',  # This is correct
            'thiocolchicoside-floctafenine': 'thiocolchicoside',  # Split compound
        }
        
        for typo, correct in typo_fixes.items():
            if typo in ingredient.lower():
                ingredient = ingredient.lower().replace(typo, correct)
        
        return ingredient.title()

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
                matches.append({
                    "match_type": "contained",
                    "confidence": 0.8,
                    "extended_ingredient": ext_data
                })
            # Check if extended name is contained in ingredient
            elif ext_name in ingredient_lower:
                matches.append({
                    "match_type": "contains",
                    "confidence": 0.7,
                    "extended_ingredient": ext_data
                })
        
        return matches[:3]  # Return top 3 matches

    def create_gemini_analysis_prompt(self, ingredients_sample: List[Dict], 
                                    extended_ingredients: Dict) -> str:
        """Create a prompt for Gemini to analyze ingredient quality and suggest mappings."""
        
        # Prepare sample data
        sample_text = []
        for ing in ingredients_sample[:20]:  # Limit to 20 for prompt size
            sample_text.append(f'"{ing["name"]}"')
        
        # Get some examples from extended ingredients
        extended_examples = list(extended_ingredients.keys())[:50]
        extended_text = '", "'.join(extended_examples)
        
        return f'''You are a pharmaceutical ingredient quality expert. Analyze these ingredient names for data quality issues and suggest proper standardized names.

PROBLEMATIC INGREDIENTS (from active_ingredients table):
{chr(10).join(sample_text)}

REFERENCE STANDARDIZED INGREDIENTS (from active_ingredients_extended):
"{extended_text}"

Return ONLY a JSON object with this structure:
{{
    "analysis": [
        {{
            "original": "exact original ingredient name",
            "issues": ["list of quality issues found"],
            "suggested_clean": "cleaned/corrected name",
            "potential_matches": ["list of matching standardized ingredients"],
            "confidence": number between 0 and 1,
            "notes": "explanation of issues and corrections"
        }}
    ]
}}

Quality Issues to Check:
1. Typos (alamond → almond, meganesium → magnesium)
2. Compound ingredients that should be split
3. Formatting issues (case, spacing, punctuation)
4. Missing or incomplete names
5. Non-standard abbreviations
6. Dosage information mixed with ingredient names

Matching Rules:
1. Look for exact matches first
2. Check for partial matches or contains relationships
3. Consider pharmaceutical synonyms and alternative names
4. Account for different forms (extract, oil, etc.)
5. Be conservative - only suggest matches you're confident about

IMPORTANT:
- Response must be ONLY valid JSON
- Include ALL input ingredients in analysis
- Be specific about quality issues found
- Provide actionable cleaning suggestions'''

    def analyze_ingredients_with_ai(self, conn, sample_ingredients: List[Dict], 
                                  extended_ingredients: Dict) -> Dict:
        """Use AI to analyze ingredient quality and suggest improvements."""
        try:
            # Create prompt
            prompt = self.create_gemini_analysis_prompt(sample_ingredients, extended_ingredients)
            
            self.logger.info(f"Analyzing {len(sample_ingredients)} ingredients with AI")
            response, _ = gemini_api.generate_content(prompt)
            
            if not response:
                raise ValueError("Empty response from Gemini API")
            
            # Clean and parse response
            cleaned_response = response.strip()
            if '```json' in cleaned_response:
                cleaned_response = cleaned_response.split('```json', 1)[1]
            elif '```' in cleaned_response:
                cleaned_response = cleaned_response.split('```', 1)[1]
            if cleaned_response.endswith('```'):
                cleaned_response = cleaned_response.rsplit('```', 1)[0]
            
            cleaned_response = cleaned_response.strip()
            analysis = json.loads(cleaned_response)
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error in AI analysis: {e}")
            return {"analysis": []}

    def analyze_sample(self, conn, sample_size: int = None) -> Dict:
        """Perform comprehensive analysis of ingredient quality."""
        if not sample_size:
            sample_size = self.sample_size
            
        self.logger.info(f"Starting ingredient quality analysis (sample size: {sample_size})")
        
        # Get data
        sample_ingredients = self.get_sample_ingredients(conn, sample_size)
        extended_ingredients = self.get_extended_ingredients(conn)
        
        self.logger.info(f"Loaded {len(sample_ingredients)} sample ingredients")
        self.logger.info(f"Loaded {len(extended_ingredients)} extended ingredients")
        
        # Analyze with AI
        ai_analysis = self.analyze_ingredients_with_ai(conn, sample_ingredients, extended_ingredients)
        
        # Combine with manual analysis
        results = {
            'timestamp': datetime.now().isoformat(),
            'sample_size': len(sample_ingredients),
            'extended_count': len(extended_ingredients),
            'ai_analysis': ai_analysis,
            'manual_analysis': self.manual_analysis(sample_ingredients, extended_ingredients)
        }
        
        # Log detailed results
        self.log_analysis_results(results)
        
        return results

    def manual_analysis(self, sample_ingredients: List[Dict], 
                       extended_ingredients: Dict) -> Dict:
        """Perform manual analysis for comparison with AI results."""
        results = {
            'exact_matches': 0,
            'fuzzy_matches': 0,
            'no_matches': 0,
            'quality_issues': {
                'likely_typos': [],
                'compound_ingredients': [],
                'formatting_issues': [],
                'missing_ingredients': []
            }
        }
        
        for ingredient in sample_ingredients:
            name = ingredient['name']
            cleaned_parts = self.clean_ingredient_name(name)
            fuzzy_matches = self.find_fuzzy_matches(name, extended_ingredients)
            
            if fuzzy_matches:
                if fuzzy_matches[0]['match_type'] == 'exact':
                    results['exact_matches'] += 1
                else:
                    results['fuzzy_matches'] += 1
            else:
                results['no_matches'] += 1
                results['quality_issues']['missing_ingredients'].append(name)
            
            # Check for quality issues
            if len(cleaned_parts) > 1:
                results['quality_issues']['compound_ingredients'].append(name)
            
            if any(char in name for char in ['(', ')', '"', '&']):
                results['quality_issues']['formatting_issues'].append(name)
        
        return results

    def log_analysis_results(self, results: Dict):
        """Log detailed analysis results."""
        # Log to main logger
        self.logger.info("=== INGREDIENT QUALITY ANALYSIS RESULTS ===")
        self.logger.info(f"Sample size: {results['sample_size']}")
        self.logger.info(f"Extended ingredients available: {results['extended_count']}")
        
        manual = results['manual_analysis']
        self.logger.info(f"Exact matches: {manual['exact_matches']}")
        self.logger.info(f"Fuzzy matches: {manual['fuzzy_matches']}")
        self.logger.info(f"No matches: {manual['no_matches']}")
        
        # Log to detailed analysis logger
        self.analysis_logger.info("DETAILED ANALYSIS RESULTS")
        self.analysis_logger.info("=" * 50)
        self.analysis_logger.info(json.dumps(results, indent=2, default=str))

    def generate_report(self, results: Dict) -> str:
        """Generate a human-readable report of the analysis."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"ingredient_quality_report_{timestamp}.txt"
        
        report_lines = [
            "INGREDIENT QUALITY ANALYSIS REPORT",
            f"Generated: {results['timestamp']}",
            f"Sample Size: {results['sample_size']}",
            f"Extended Ingredients Available: {results['extended_count']}",
            "",
            "=== SUMMARY ===",
        ]
        
        manual = results['manual_analysis']
        total = results['sample_size']
        
        report_lines.extend([
            f"Exact matches: {manual['exact_matches']} ({manual['exact_matches']/total*100:.1f}%)",
            f"Fuzzy matches: {manual['fuzzy_matches']} ({manual['fuzzy_matches']/total*100:.1f}%)",
            f"No matches: {manual['no_matches']} ({manual['no_matches']/total*100:.1f}%)",
            "",
            "=== QUALITY ISSUES ===",
            f"Compound ingredients: {len(manual['quality_issues']['compound_ingredients'])}",
            f"Formatting issues: {len(manual['quality_issues']['formatting_issues'])}",
            f"Missing ingredients: {len(manual['quality_issues']['missing_ingredients'])}",
            "",
        ])
        
        # Add AI analysis if available
        if 'analysis' in results['ai_analysis']:
            report_lines.extend([
                "=== AI ANALYSIS HIGHLIGHTS ===",
                ""
            ])
            
            for item in results['ai_analysis']['analysis'][:10]:  # Top 10
                report_lines.extend([
                    f"Original: {item['original']}",
                    f"Issues: {', '.join(item['issues'])}",
                    f"Suggested: {item['suggested_clean']}",
                    f"Confidence: {item['confidence']}",
                    f"Notes: {item['notes']}",
                    ""
                ])
        
        # Write report
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        self.logger.info(f"Report generated: {report_file}")
        return report_file


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Analyze ingredient quality and suggest improvements")
    parser.add_argument('--sample', type=int, default=50, help='Sample size for analysis')
    parser.add_argument('--full', action='store_true', help='Analyze all ingredients (may take long time)')
    args = parser.parse_args()
    
    try:
        # Initialize Gemini API
        gemini_api.initialize_gemini()
        
        # Initialize analyzer
        sample_size = None if args.full else args.sample
        analyzer = IngredientQualityAnalyzer(sample_size or 50)
        
        # Connect to database
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST
        )
        
        try:
            # Perform analysis
            results = analyzer.analyze_sample(conn, sample_size)
            
            # Generate report
            report_file = analyzer.generate_report(results)
            
            print(f"\nAnalysis complete!")
            print(f"Report saved to: {report_file}")
            print(f"Check logs for detailed information")
            
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()