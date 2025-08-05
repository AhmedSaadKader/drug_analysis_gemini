"""
Claude Interactive Mapping Interface for Flask-Admin
Real-time ingredient mapping with Claude integration
"""

import sys
import os
import json
from datetime import datetime
from typing import List, Dict, Optional

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from flask import request, flash, redirect, url_for, jsonify, render_template, session
from flask_admin import BaseView, expose
from sqlalchemy import text

from models import db, ActiveIngredient, ActiveIngredientExtended, IngredientMapping, IngredientMappingLog


class ClaudeMappingView(BaseView):
    """
    Flask-Admin view for Claude Interactive Mapping
    Provides web interface for real-time ingredient mapping with Claude
    """
    
    def __init__(self, name='Claude Mapper', endpoint='claude_mapper', menu_icon_type='fa', menu_icon_value='fa-brain'):
        super().__init__(name, endpoint, menu_icon_type=menu_icon_type, menu_icon_value=menu_icon_value)
    
    @expose('/')
    def index(self):
        """Main Claude mapping interface"""
        # Get session statistics
        stats = self._get_mapping_statistics()
        
        # Get current mapping session if exists
        mapping_session = session.get('claude_mapping_session', {})
        
        return self.render('claude_mapper/index.html', 
                         stats=stats, 
                         mapping_session=mapping_session)
    
    @expose('/start_session', methods=['GET', 'POST'])
    def start_session(self):
        """Start new Claude mapping session"""
        if request.method == 'POST':
            batch_size = int(request.form.get('batch_size', 20))
            session_name = request.form.get('session_name', f'Session_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
            processing_mode = request.form.get('processing_mode', 'interactive')
            auto_create = request.form.get('auto_create') == 'on'
            compound_splitting = request.form.get('compound_splitting') == 'on'
            
            # Get unprocessed ingredients
            ingredients = self._get_unprocessed_ingredients(batch_size)
            
            if not ingredients:
                flash('No unprocessed ingredients found!', 'warning')
                return redirect(url_for('claudemappingview.index'))
            
            # Create mapping session
            mapping_session = {
                'id': datetime.now().strftime('%Y%m%d_%H%M%S'),
                'name': session_name,
                'batch_size': batch_size,
                'processing_mode': processing_mode,
                'auto_create': auto_create,
                'compound_splitting': compound_splitting,
                'ingredients': ingredients,
                'current_index': 0,
                'completed_mappings': [],
                'auto_processed': [],  # Track automatically processed items
                'created_at': datetime.now().isoformat(),
                'status': 'active'
            }
            
            session['claude_mapping_session'] = mapping_session
            
            # If smart mode, process high-confidence items automatically
            if processing_mode == 'smart':
                auto_processed_count = self._auto_process_high_confidence(mapping_session)
                if auto_processed_count > 0:
                    flash(f'Started smart mapping session: {auto_processed_count} high-confidence items auto-processed, {len(ingredients) - auto_processed_count} require review', 'success')
                else:
                    flash(f'Started smart mapping session with {len(ingredients)} ingredients (none auto-processed)', 'success')
            else:
                flash(f'Started interactive mapping session with {len(ingredients)} ingredients', 'success')
            
            return redirect(url_for('claudemappingview.mapping_interface'))
        
        # GET request - show session configuration
        unmapped_count = self._get_unmapped_count()
        return self.render('claude_mapper/start_session.html', unmapped_count=unmapped_count)
    
    @expose('/mapping_interface')
    def mapping_interface(self):
        """Interactive mapping interface for Claude analysis"""
        mapping_session = session.get('claude_mapping_session')
        
        if not mapping_session or mapping_session.get('status') != 'active':
            flash('No active mapping session found. Please start a new session.', 'warning')
            return redirect(url_for('claudemappingview.start_session'))
        
        current_index = mapping_session.get('current_index', 0)
        ingredients = mapping_session.get('ingredients', [])
        
        if current_index >= len(ingredients):
            # Check if we have high-confidence items for final review
            high_confidence_items = self._get_high_confidence_items_for_review(mapping_session)
            if high_confidence_items:
                flash('All ingredients processed! Review high-confidence items for final approval.', 'info')
                return redirect(url_for('claudemappingview.final_review'))
            else:
                flash('All ingredients in this session have been processed!', 'info')
                return redirect(url_for('claudemappingview.session_complete'))
        
        # Get current ingredient and similar matches
        current_ingredient = ingredients[current_index]
        similar_ingredients = self._search_similar_ingredients(current_ingredient['name'])
        
        # Get Claude's AI suggestion
        claude_suggestion = self._get_claude_suggestion(current_ingredient, similar_ingredients)
        
        # Prepare data for Claude analysis
        ingredient_data = {
            'current': current_ingredient,
            'similar': similar_ingredients,
            'claude_suggestion': claude_suggestion,
            'session_progress': {
                'current': current_index + 1,
                'total': len(ingredients),
                'percentage': round((current_index / len(ingredients)) * 100, 1)
            }
        }
        
        return self.render('claude_mapper/mapping_interface.html', 
                         ingredient_data=ingredient_data,
                         mapping_session=mapping_session)
    
    @expose('/process_mapping', methods=['POST'])
    def process_mapping(self):
        """Process Claude's mapping decision"""
        mapping_session = session.get('claude_mapping_session')
        
        if not mapping_session or mapping_session.get('status') != 'active':
            return jsonify({'error': 'No active mapping session'}), 400
        
        # Get form data
        action = request.form.get('action')
        ingredient_id = request.form.get('ingredient_id')
        confidence = float(request.form.get('confidence', 1.0))
        notes = request.form.get('notes', '')
        
        try:
            result = None
            
            # Get original ingredient name for better tracking
            raw_ingredient = ActiveIngredient.query.get(ingredient_id)
            original_name = raw_ingredient.name if raw_ingredient else 'Unknown'
            
            if action == 'MAP_TO_EXISTING':
                target_id = request.form.get('target_id')
                result = self._create_mapping(ingredient_id, target_id, confidence, 'claude_interactive', notes, original_name)
                
            elif action == 'CREATE_NEW':
                new_name = request.form.get('new_name')
                new_description = request.form.get('new_description')
                new_uses = request.form.get('new_uses', '')
                
                # Create new ingredient
                new_ingredient_id = self._create_new_ingredient(new_name, new_description, new_uses)
                # Create mapping
                result = self._create_mapping(ingredient_id, new_ingredient_id, confidence, 'claude_interactive', notes, original_name)
                
            elif action == 'COMPOUND_SPLIT':
                # Handle compound splitting
                component_names = request.form.getlist('component_name[]')
                component_descs = request.form.getlist('component_desc[]')
                
                for i, comp_name in enumerate(component_names):
                    if comp_name.strip():
                        comp_desc = component_descs[i] if i < len(component_descs) else ''
                        
                        # Create new ingredient for each component
                        new_id = self._create_new_ingredient(comp_name, comp_desc, '')
                        
                        # Create mapping with compound split notes
                        compound_notes = f"Compound component: {comp_name}\n{notes}"
                        self._create_mapping(ingredient_id, new_id, confidence, 'claude_compound_split', compound_notes, original_name)
                        
                result = {'status': 'compound_split_complete'}
                
            elif action == 'NO_MAPPING':
                # Log as unmappable with detailed reasoning
                unmappable_notes = f"No mapping possible - {original_name}\nReason: {notes}"
                self._log_unmappable_ingredient(ingredient_id, unmappable_notes, original_name)
                result = {'status': 'no_mapping_logged'}
                
            elif action == 'SKIP':
                # Skip ingredient - just move to next without any mapping
                result = {'status': 'ingredient_skipped'}
                # Don't log anything, just continue to next ingredient
            
            # Update session progress
            current_index = mapping_session.get('current_index', 0)
            mapping_session['current_index'] = current_index + 1
            mapping_session['completed_mappings'].append({
                'ingredient_id': ingredient_id,
                'action': action,
                'timestamp': datetime.now().isoformat(),
                'result': result
            })
            
            session['claude_mapping_session'] = mapping_session
            
            flash(f'Successfully processed mapping: {action}', 'success')
            
            # Check if there are more ingredients to process
            current_index = mapping_session.get('current_index', 0)
            total_ingredients = len(mapping_session.get('ingredients', []))
            
            if current_index >= total_ingredients:
                # Session complete
                return redirect(url_for('claudemappingview.session_complete'))
            else:
                # Continue to next ingredient
                return redirect(url_for('claudemappingview.mapping_interface'))
            
        except Exception as e:
            error_msg = str(e)
            
            # Handle duplicate mapping error specifically
            if 'duplicate key value violates unique constraint' in error_msg:
                flash('This ingredient has already been mapped. Skipping to next ingredient.', 'info')
                
                # Update session to skip this ingredient
                current_index = mapping_session.get('current_index', 0)
                mapping_session['current_index'] = current_index + 1
                session['claude_mapping_session'] = mapping_session
                
                # Check if session is complete
                total_ingredients = len(mapping_session.get('ingredients', []))
                if mapping_session['current_index'] >= total_ingredients:
                    return redirect(url_for('claudemappingview.session_complete'))
                else:
                    return redirect(url_for('claudemappingview.mapping_interface'))
            else:
                flash(f'Error processing mapping: {error_msg}', 'error')
                return redirect(url_for('claudemappingview.mapping_interface'))
    
    @expose('/final_review')
    def final_review(self):
        """Final review of high-confidence items before session completion"""
        mapping_session = session.get('claude_mapping_session')
        
        if not mapping_session:
            flash('No mapping session found.', 'warning')
            return redirect(url_for('claudemappingview.index'))
        
        # Get high-confidence items for review
        high_confidence_items = self._get_high_confidence_items_for_review(mapping_session)
        
        if not high_confidence_items:
            # No high-confidence items, go straight to completion
            return redirect(url_for('claudemappingview.session_complete'))
        
        return self.render('claude_mapper/final_review.html', 
                         mapping_session=mapping_session,
                         high_confidence_items=high_confidence_items)
    
    @expose('/bulk_approve', methods=['POST'])
    def bulk_approve(self):
        """Bulk approve selected high-confidence items"""
        mapping_session = session.get('claude_mapping_session')
        
        if not mapping_session:
            return jsonify({'error': 'No active session'}), 400
        
        selected_items = request.form.getlist('selected_items')
        if not selected_items:
            flash('No items selected for approval.', 'warning')
            return redirect(url_for('claudemappingview.final_review'))
        
        try:
            approved_count = 0
            
            for item_data in selected_items:
                item = json.loads(item_data)
                
                # Get original ingredient name
                raw_ingredient = ActiveIngredient.query.get(item['ingredient']['id'])
                original_name = raw_ingredient.name if raw_ingredient else item['ingredient']['name']
                
                # Process based on suggestion action
                if item['suggestion']['action'] == 'MAP_TO_EXISTING' and item['suggestion'].get('target_ingredient'):
                    self._create_mapping(
                        item['ingredient']['id'], 
                        item['suggestion']['target_ingredient']['id'], 
                        item['suggestion']['confidence'], 
                        'claude_bulk_approved', 
                        f"Bulk approved - {item['suggestion']['reasoning']}", 
                        original_name
                    )
                    approved_count += 1
                    
                elif item['suggestion']['action'] == 'CREATE_NEW' and item['suggestion'].get('suggested_name'):
                    # Create new ingredient
                    new_ingredient_id = self._create_new_ingredient(
                        item['suggestion']['suggested_name'],
                        item['suggestion'].get('suggested_description', ''),
                        ''
                    )
                    # Create mapping
                    self._create_mapping(
                        item['ingredient']['id'],
                        new_ingredient_id,
                        item['suggestion']['confidence'],
                        'claude_bulk_approved',
                        f"Bulk approved - {item['suggestion']['reasoning']}",
                        original_name
                    )
                    approved_count += 1
                    
                elif item['suggestion']['action'] == 'NO_MAPPING':
                    # Log as unmappable
                    self._log_unmappable_ingredient(
                        item['ingredient']['id'],
                        f"Bulk approved - {item['suggestion']['reasoning']}",
                        original_name
                    )
                    approved_count += 1
            
            # Update session to mark these as processed
            mapping_session['bulk_approved_count'] = approved_count
            session['claude_mapping_session'] = mapping_session
            
            flash(f'Successfully approved {approved_count} high-confidence mappings!', 'success')
            return redirect(url_for('claudemappingview.session_complete'))
            
        except Exception as e:
            flash(f'Error during bulk approval: {str(e)}', 'error')
            return redirect(url_for('claudemappingview.final_review'))

    @expose('/session_complete')
    def session_complete(self):
        """Session completion summary"""
        mapping_session = session.get('claude_mapping_session')
        
        if not mapping_session:
            flash('No mapping session found.', 'warning')
            return redirect(url_for('claudemappingview.index'))
        
        # Mark session as complete
        mapping_session['status'] = 'completed'
        mapping_session['completed_at'] = datetime.now().isoformat()
        session['claude_mapping_session'] = mapping_session
        
        # Generate completion statistics
        completion_stats = self._generate_completion_stats(mapping_session)
        
        return self.render('claude_mapper/session_complete.html', 
                         mapping_session=mapping_session,
                         completion_stats=completion_stats)
    
    @expose('/api/similar_ingredients')
    def api_similar_ingredients(self):
        """API endpoint for searching similar ingredients"""
        search_term = request.args.get('q', '')
        limit = int(request.args.get('limit', 10))
        
        if not search_term:
            return jsonify([])
        
        similar = self._search_similar_ingredients(search_term, limit)
        return jsonify(similar)
    
    # Helper methods
    
    def _get_mapping_statistics(self) -> Dict:
        """Get current mapping statistics"""
        total_raw = ActiveIngredient.query.count()
        total_extended = ActiveIngredientExtended.query.count()
        total_mappings = IngredientMapping.query.count()
        unmapped_count = self._get_unmapped_count()
        
        # Claude-specific statistics
        claude_mappings = IngredientMapping.query.filter(
            IngredientMapping.mapping_type.like('%claude%')
        ).count()
        
        avg_confidence = db.session.query(
            db.func.avg(IngredientMapping.confidence)
        ).filter(
            IngredientMapping.mapping_type.like('%claude%')
        ).scalar() or 0
        
        return {
            'total_raw_ingredients': total_raw,
            'total_extended_ingredients': total_extended,
            'total_mappings': total_mappings,
            'unmapped_ingredients': unmapped_count,
            'claude_mappings': claude_mappings,
            'claude_avg_confidence': float(avg_confidence),
            'mapping_coverage': round((total_mappings / total_raw) * 100, 1) if total_raw > 0 else 0
        }
    
    def _get_unmapped_count(self) -> int:
        """Get count of unmapped ingredients"""
        query = text("""
            SELECT COUNT(*)
            FROM active_ingredients ai
            LEFT JOIN ingredient_mappings im ON ai.ingredient_id = im.raw_ingredient_id
            WHERE im.raw_ingredient_id IS NULL
        """)
        
        result = db.session.execute(query).scalar()
        return result or 0
    
    def _get_unprocessed_ingredients(self, limit: int = 20) -> List[Dict]:
        """Get unprocessed ingredients ordered by frequency"""
        query = text("""
            SELECT ai.ingredient_id, ai.name, 
                   COALESCE((SELECT COUNT(*) FROM product_ingredients pi WHERE pi.ingredient_id = ai.ingredient_id), 0) as frequency
            FROM active_ingredients ai
            LEFT JOIN ingredient_mappings im ON ai.ingredient_id = im.raw_ingredient_id
            WHERE im.raw_ingredient_id IS NULL
            ORDER BY frequency DESC
            LIMIT :limit
        """)
        
        result = db.session.execute(query, {'limit': limit}).fetchall()
        
        ingredients = []
        for row in result:
            ingredients.append({
                'id': row[0],
                'name': row[1],
                'frequency': row[2]
            })
        
        return ingredients
    
    def _search_similar_ingredients(self, search_term: str, limit: int = 10) -> List[Dict]:
        """Search for similar ingredients in extended database"""
        search_lower = search_term.lower()
        
        query = text("""
            SELECT ingredient_name, short_description, id
            FROM active_ingredients_extended
            WHERE LOWER(ingredient_name) LIKE :exact_match
               OR LOWER(ingredient_name) LIKE :starts_with  
               OR LOWER(ingredient_name) LIKE :contains
               OR LOWER(short_description) LIKE :contains
            ORDER BY 
                CASE 
                    WHEN LOWER(ingredient_name) = :exact_lower THEN 1
                    WHEN LOWER(ingredient_name) LIKE :starts_with THEN 2
                    WHEN LOWER(ingredient_name) LIKE :contains THEN 3
                    ELSE 4
                END,
                LENGTH(ingredient_name)
            LIMIT :limit
        """)
        
        params = {
            'exact_match': search_lower,
            'starts_with': f'{search_lower}%',
            'contains': f'%{search_lower}%',
            'exact_lower': search_lower,
            'limit': limit
        }
        
        result = db.session.execute(query, params).fetchall()
        
        similar = []
        for row in result:
            similar.append({
                'name': row[0],
                'description': row[1] or '',
                'id': row[2]
            })
        
        return similar
    
    def _create_mapping(self, raw_id: str, extended_id: int, confidence: float, method: str, notes: str = '', original_ingredient_name: str = '') -> Dict:
        """Create ingredient mapping"""
        # Check if mapping already exists
        existing_mapping = IngredientMapping.query.filter_by(
            raw_ingredient_id=raw_id,
            extended_ingredient_id=extended_id
        ).first()
        
        if existing_mapping:
            # Update existing mapping instead of creating duplicate
            existing_mapping.confidence = max(existing_mapping.confidence, confidence)  # Keep higher confidence
            existing_mapping.extraction_method = method
            existing_mapping.verified = True
            existing_mapping.verified_by = 'claude'
            existing_mapping.verified_at = datetime.utcnow()
            existing_mapping.updated_at = datetime.utcnow()
            
            # Update notes to include new attempt
            if not original_ingredient_name:
                raw_ingredient = ActiveIngredient.query.get(raw_id)
                original_ingredient_name = raw_ingredient.name if raw_ingredient else 'Unknown'
            
            additional_notes = f"\n--- Updated {datetime.utcnow().isoformat()} ---\n"
            additional_notes += f"Re-processed via {method}\n"
            if notes:
                additional_notes += f"Additional notes: {notes}\n"
            additional_notes += f"Updated confidence: {confidence:.2f}"
            
            existing_mapping.ai_notes = (existing_mapping.ai_notes or '') + additional_notes
            
            db.session.commit()
            
            return {'status': 'mapping_updated', 'mapping_id': existing_mapping.id}
        
        # Get the original ingredient name for better tracking
        if not original_ingredient_name:
            raw_ingredient = ActiveIngredient.query.get(raw_id)
            original_ingredient_name = raw_ingredient.name if raw_ingredient else 'Unknown'
        
        # Get extended ingredient name for detailed notes
        extended_ingredient = ActiveIngredientExtended.query.get(extended_id)
        extended_name = extended_ingredient.ingredient_name if extended_ingredient else 'Unknown'
        
        # Create comprehensive AI notes
        comprehensive_notes = f"Claude Interactive Mapping:\n"
        comprehensive_notes += f"Original: '{original_ingredient_name}'\n"
        comprehensive_notes += f"Mapped to: '{extended_name}'\n"
        comprehensive_notes += f"Method: {method}\n"
        if notes:
            comprehensive_notes += f"User notes: {notes}\n"
        comprehensive_notes += f"Processed via web interface with confidence: {confidence:.2f}"
        
        mapping = IngredientMapping(
            raw_ingredient_id=raw_id,
            extended_ingredient_id=extended_id,
            mapping_type='claude_interactive',
            confidence=confidence,
            extraction_method=method,
            original_text=original_ingredient_name,  # Store original ingredient name
            ai_notes=comprehensive_notes,  # Store detailed processing notes
            created_by='claude_web_interface',
            verified=True,  # Claude mappings are pre-verified
            verified_by='claude',
            verified_at=datetime.utcnow()
        )
        
        db.session.add(mapping)
        db.session.commit()
        
        return {'status': 'mapping_created', 'mapping_id': mapping.id}
    
    def _create_new_ingredient(self, name: str, description: str, uses: str = '') -> int:
        """Create new ingredient in extended table"""
        ingredient = ActiveIngredientExtended(
            ingredient_name=name,
            short_description=description,
            common_uses=uses,
            processing_status='claude_created',
            last_updated=datetime.utcnow()
        )
        
        db.session.add(ingredient)
        db.session.flush()  # Get ID
        db.session.commit()
        
        return ingredient.id
    
    def _log_unmappable_ingredient(self, ingredient_id: str, reason: str, original_name: str = '') -> None:
        """Log ingredient as unmappable with detailed information"""
        # Create a mapping entry with special handling for unmappable items
        # This helps track what was attempted and why it failed
        
        # Create a detailed log entry
        log_entry = {
            'ingredient_id': ingredient_id,
            'original_name': original_name,
            'reason': reason,
            'timestamp': datetime.utcnow().isoformat(),
            'processor': 'claude_web_interface',
            'action': 'NO_MAPPING'
        }
        
        # You could store this in a separate unmappable_ingredients table
        # or in the application logs for tracking
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Unmappable ingredient logged: {log_entry}")
        
        # For now, we'll track these in session for reporting
        # In a production system, you might want a dedicated table
    
    def _get_claude_suggestion(self, ingredient: Dict, similar_ingredients: List[Dict]) -> Dict:
        """Get Claude's AI suggestion for ingredient mapping"""
        ingredient_name = ingredient['name']
        frequency = ingredient['frequency']
        
        # Analyze ingredient name for pharmaceutical context
        suggestion = {
            'action': 'CREATE_NEW',  # Default
            'confidence': 0.8,
            'reasoning': '',
            'suggested_name': '',
            'suggested_description': '',
            'target_ingredient': None,
            'components': []
        }
        
        # Check for exact or very close matches
        if similar_ingredients:
            best_match = similar_ingredients[0]
            name_similarity = self._calculate_similarity(ingredient_name.lower(), best_match['name'].lower())
            
            if name_similarity > 0.9:  # Very close match
                suggestion.update({
                    'action': 'MAP_TO_EXISTING',
                    'confidence': min(0.95, name_similarity),
                    'reasoning': f'Very close match found: "{best_match["name"]}" (similarity: {name_similarity:.1%})',
                    'target_ingredient': best_match
                })
            elif name_similarity > 0.7:  # Good match
                suggestion.update({
                    'action': 'MAP_TO_EXISTING',
                    'confidence': 0.8,
                    'reasoning': f'Good match found: "{best_match["name"]}" (similarity: {name_similarity:.1%})',
                    'target_ingredient': best_match
                })
        
        # Check for compound ingredients (contains +, /, &, "and", etc.)
        compound_indicators = ['+', '/', '&', ' and ', ' with ', ',']
        if any(indicator in ingredient_name.lower() for indicator in compound_indicators):
            components = self._analyze_compound(ingredient_name)
            if len(components) > 1:
                suggestion.update({
                    'action': 'COMPOUND_SPLIT',
                    'confidence': 0.85,
                    'reasoning': f'Compound ingredient detected with {len(components)} components',
                    'components': components
                })
        
        # Check for non-pharmaceutical ingredients
        non_pharma_keywords = ['color', 'dye', 'flavor', 'fragrance', 'preservative', 'inactive']
        if any(keyword in ingredient_name.lower() for keyword in non_pharma_keywords):
            suggestion.update({
                'action': 'NO_MAPPING',
                'confidence': 0.9,
                'reasoning': 'Appears to be non-pharmaceutical ingredient (coloring, flavoring, etc.)'
            })
        
        # If creating new, suggest standardized name and description
        if suggestion['action'] == 'CREATE_NEW':
            standardized_name = self._standardize_ingredient_name(ingredient_name)
            description = self._generate_ingredient_description(standardized_name)
            
            suggestion.update({
                'suggested_name': standardized_name,
                'suggested_description': description,
                'reasoning': f'No suitable match found. Suggested standardized name: "{standardized_name}"'
            })
        
        return suggestion
    
    def _calculate_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two ingredient names"""
        from difflib import SequenceMatcher
        return SequenceMatcher(None, name1, name2).ratio()
    
    def _analyze_compound(self, ingredient_name: str) -> List[Dict]:
        """Analyze compound ingredient and extract components"""
        # Simple compound splitting logic
        separators = [' + ', ' / ', ' & ', ' and ', ' with ', ', ']
        components = [ingredient_name.strip()]
        
        for sep in separators:
            if sep in ingredient_name:
                components = [comp.strip() for comp in ingredient_name.split(sep)]
                break
        
        # Return component analysis
        result = []
        for comp in components:
            if comp:
                standardized = self._standardize_ingredient_name(comp)
                result.append({
                    'original': comp,
                    'standardized': standardized,
                    'description': self._generate_ingredient_description(standardized)
                })
        
        return result
    
    def _standardize_ingredient_name(self, name: str) -> str:
        """Standardize ingredient name"""
        # Basic standardization rules
        standardized = name.strip()
        
        # Remove common suffixes/prefixes
        removals = ['ingredient:', 'active:', 'contains:', 'including:']
        for removal in removals:
            if standardized.lower().startswith(removal):
                standardized = standardized[len(removal):].strip()
        
        # Capitalize properly
        standardized = ' '.join(word.capitalize() for word in standardized.split())
        
        # Handle common pharmaceutical terms
        replacements = {
            'Hcl': 'HCl',
            'Mg': 'mg',
            'Mcg': 'mcg',
            'Iu': 'IU',
            'Usp': 'USP'
        }
        
        for old, new in replacements.items():
            standardized = standardized.replace(old, new)
        
        return standardized
    
    def _generate_ingredient_description(self, name: str) -> str:
        """Generate basic description for ingredient"""
        name_lower = name.lower()
        
        # Common pharmaceutical descriptions
        descriptions = {
            'acetaminophen': 'Analgesic and antipyretic medication used for pain relief and fever reduction',
            'ibuprofen': 'Nonsteroidal anti-inflammatory drug (NSAID) used for pain, fever, and inflammation',
            'aspirin': 'Salicylate medication used for pain, fever, inflammation, and cardiovascular protection',
            'caffeine': 'Central nervous system stimulant used to enhance alertness and treat headaches',
            'diphenhydramine': 'Antihistamine used for allergies, sleep aid, and motion sickness'
        }
        
        # Check for exact matches
        for key, desc in descriptions.items():
            if key in name_lower:
                return desc
        
        # Generate generic description based on common patterns
        if 'acid' in name_lower:
            return 'Pharmaceutical acid compound with therapeutic properties'
        elif 'sodium' in name_lower:
            return 'Sodium salt form of pharmaceutical compound for improved bioavailability'
        elif 'hydrochloride' in name_lower or 'hcl' in name_lower:
            return 'Hydrochloride salt form of pharmaceutical compound'
        elif 'oxide' in name_lower:
            return 'Oxide form of pharmaceutical compound'
        elif 'extract' in name_lower:
            return 'Natural extract with pharmaceutical properties'
        else:
            return f'Pharmaceutical ingredient: {name}'
    
    def _generate_completion_stats(self, mapping_session: Dict) -> Dict:
        """Generate completion statistics for session"""
        completed = mapping_session.get('completed_mappings', [])
        auto_processed = mapping_session.get('auto_processed', [])
        
        # Combine manual and auto-processed items
        all_processed = completed + auto_processed
        
        # Include bulk approved items
        bulk_approved_count = mapping_session.get('bulk_approved_count', 0)
        
        stats = {
            'total_processed': len(all_processed) + bulk_approved_count,
            'manual_processed': len(completed),
            'auto_processed': len(auto_processed),
            'bulk_approved': bulk_approved_count,
            'actions': {},
            'success_rate': 0,
            'avg_confidence': 0
        }
        
        # Count actions from both manual and auto-processed
        for mapping in completed:
            action = mapping.get('action', 'unknown')
            stats['actions'][action] = stats['actions'].get(action, 0) + 1
            
        for auto_item in auto_processed:
            action = auto_item.get('action', 'unknown')
            stats['actions'][action] = stats['actions'].get(action, 0) + 1
        
        # Calculate success rate (non-NO_MAPPING actions)
        successful = sum(count for action, count in stats['actions'].items() if action != 'NO_MAPPING')
        stats['success_rate'] = round((successful / len(all_processed)) * 100, 1) if all_processed else 0
        
        return stats
    
    def _auto_process_high_confidence(self, mapping_session: Dict) -> int:
        """Auto-process high-confidence mappings in smart mode"""
        ingredients = mapping_session.get('ingredients', [])
        auto_processed = []
        
        for ingredient in ingredients:
            # Get Claude's suggestion for this ingredient
            similar_ingredients = self._search_similar_ingredients(ingredient['name'])
            claude_suggestion = self._get_claude_suggestion(ingredient, similar_ingredients)
            
            # Auto-process if confidence >= 90%
            if claude_suggestion['confidence'] >= 0.9:
                try:
                    # Get original ingredient name
                    raw_ingredient = ActiveIngredient.query.get(ingredient['id'])
                    original_name = raw_ingredient.name if raw_ingredient else ingredient['name']
                    
                    # Process based on suggestion action
                    if claude_suggestion['action'] == 'MAP_TO_EXISTING' and claude_suggestion.get('target_ingredient'):
                        self._create_mapping(
                            ingredient['id'], 
                            claude_suggestion['target_ingredient']['id'], 
                            claude_suggestion['confidence'], 
                            'claude_smart_auto', 
                            f"Auto-processed in smart mode - {claude_suggestion['reasoning']}", 
                            original_name
                        )
                        
                    elif claude_suggestion['action'] == 'CREATE_NEW' and claude_suggestion.get('suggested_name'):
                        # Create new ingredient
                        new_ingredient_id = self._create_new_ingredient(
                            claude_suggestion['suggested_name'],
                            claude_suggestion.get('suggested_description', ''),
                            ''
                        )
                        # Create mapping
                        self._create_mapping(
                            ingredient['id'],
                            new_ingredient_id,
                            claude_suggestion['confidence'],
                            'claude_smart_auto',
                            f"Auto-processed in smart mode - {claude_suggestion['reasoning']}",
                            original_name
                        )
                        
                    elif claude_suggestion['action'] == 'NO_MAPPING':
                        # Log as unmappable
                        self._log_unmappable_ingredient(
                            ingredient['id'],
                            f"Auto-processed in smart mode - {claude_suggestion['reasoning']}",
                            original_name
                        )
                    
                    # Track auto-processed item
                    auto_processed.append({
                        'ingredient': ingredient,
                        'action': claude_suggestion['action'],
                        'confidence': claude_suggestion['confidence'],
                        'reasoning': claude_suggestion['reasoning'],
                        'processed_at': datetime.now().isoformat()
                    })
                    
                except Exception as e:
                    # If auto-processing fails, leave for manual review
                    print(f"Auto-processing failed for {ingredient['name']}: {e}")
                    continue
        
        # Update session with auto-processed items
        mapping_session['auto_processed'] = auto_processed
        mapping_session['current_index'] = len(auto_processed)  # Skip auto-processed items
        
        # Update session in Flask session
        session['claude_mapping_session'] = mapping_session
        
        return len(auto_processed)
    
    def _get_high_confidence_items_for_review(self, mapping_session: Dict) -> List[Dict]:
        """Get remaining unprocessed ingredients with 100% confidence for final review"""
        ingredients = mapping_session.get('ingredients', [])
        current_index = mapping_session.get('current_index', 0)
        
        # Get remaining unprocessed ingredients
        remaining_ingredients = ingredients[current_index:]
        
        high_confidence_items = []
        
        for ingredient in remaining_ingredients:
            # Check if already mapped (skip if so)
            existing_mapping = IngredientMapping.query.filter_by(
                raw_ingredient_id=ingredient['id']
            ).first()
            
            if existing_mapping:
                continue  # Skip already mapped ingredients
            
            # Get Claude's suggestion for this ingredient
            similar_ingredients = self._search_similar_ingredients(ingredient['name'])
            claude_suggestion = self._get_claude_suggestion(ingredient, similar_ingredients)
            
            # Only include 100% confidence items
            if claude_suggestion['confidence'] >= 1.0:
                high_confidence_items.append({
                    'ingredient': ingredient,
                    'suggestion': claude_suggestion,
                    'similar': similar_ingredients
                })
        
        return high_confidence_items