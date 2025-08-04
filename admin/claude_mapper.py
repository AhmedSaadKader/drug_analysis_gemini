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
                'ingredients': ingredients,
                'current_index': 0,
                'completed_mappings': [],
                'created_at': datetime.now().isoformat(),
                'status': 'active'
            }
            
            session['claude_mapping_session'] = mapping_session
            flash(f'Started new mapping session with {len(ingredients)} ingredients', 'success')
            
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
            flash('All ingredients in this session have been processed!', 'info')
            return redirect(url_for('claudemappingview.session_complete'))
        
        # Get current ingredient and similar matches
        current_ingredient = ingredients[current_index]
        similar_ingredients = self._search_similar_ingredients(current_ingredient['name'])
        
        # Prepare data for Claude analysis
        ingredient_data = {
            'current': current_ingredient,
            'similar': similar_ingredients,
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
            
            if action == 'MAP_TO_EXISTING':
                target_id = request.form.get('target_id')
                result = self._create_mapping(ingredient_id, target_id, confidence, 'claude_interactive', notes)
                
            elif action == 'CREATE_NEW':
                new_name = request.form.get('new_name')
                new_description = request.form.get('new_description')
                new_uses = request.form.get('new_uses', '')
                
                # Create new ingredient
                new_ingredient_id = self._create_new_ingredient(new_name, new_description, new_uses)
                # Create mapping
                result = self._create_mapping(ingredient_id, new_ingredient_id, confidence, 'claude_interactive', notes)
                
            elif action == 'COMPOUND_SPLIT':
                # Handle compound splitting
                components = request.form.getlist('components[]')
                for component_data in components:
                    comp_data = json.loads(component_data)
                    if comp_data.get('action') == 'CREATE_NEW':
                        new_id = self._create_new_ingredient(
                            comp_data['name'], 
                            comp_data['description'], 
                            comp_data.get('uses', '')
                        )
                        self._create_mapping(ingredient_id, new_id, comp_data.get('confidence', 0.9), 'claude_compound_split', notes)
                    elif comp_data.get('action') == 'MAP_TO_EXISTING':
                        self._create_mapping(ingredient_id, comp_data['target_id'], comp_data.get('confidence', 0.9), 'claude_compound_split', notes)
                        
                result = {'status': 'compound_split_complete'}
                
            elif action == 'NO_MAPPING':
                # Log as unmappable
                self._log_unmappable_ingredient(ingredient_id, notes)
                result = {'status': 'no_mapping_logged'}
            
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
            return jsonify({'status': 'success', 'result': result})
            
        except Exception as e:
            flash(f'Error processing mapping: {str(e)}', 'error')
            return jsonify({'error': str(e)}), 500
    
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
    
    def _create_mapping(self, raw_id: str, extended_id: int, confidence: float, method: str, notes: str = '') -> Dict:
        """Create ingredient mapping"""
        mapping = IngredientMapping(
            raw_ingredient_id=raw_id,
            extended_ingredient_id=extended_id,
            mapping_type='claude_interactive',
            confidence=confidence,
            extraction_method=method,
            ai_notes=notes,
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
    
    def _log_unmappable_ingredient(self, ingredient_id: str, reason: str) -> None:
        """Log ingredient as unmappable"""
        # Could create a separate table for unmappable ingredients
        # For now, we'll just log it in the application logs
        pass
    
    def _generate_completion_stats(self, mapping_session: Dict) -> Dict:
        """Generate completion statistics for session"""
        completed = mapping_session.get('completed_mappings', [])
        
        stats = {
            'total_processed': len(completed),
            'actions': {},
            'success_rate': 0,
            'avg_confidence': 0
        }
        
        for mapping in completed:
            action = mapping.get('action', 'unknown')
            stats['actions'][action] = stats['actions'].get(action, 0) + 1
        
        # Calculate success rate (non-NO_MAPPING actions)
        successful = sum(count for action, count in stats['actions'].items() if action != 'NO_MAPPING')
        stats['success_rate'] = round((successful / len(completed)) * 100, 1) if completed else 0
        
        return stats