"""
Drug Analysis Admin Interface
Flask-Admin application for managing pharmaceutical ingredient mappings
"""
import os
import sys
from datetime import datetime

# Add current directory to Python path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from flask import Flask, redirect, url_for, request, flash
from flask_admin import Admin, AdminIndexView, expose
from flask_admin.contrib.sqla import ModelView
from flask_admin.model import typefmt

from config import config
from models import db, ActiveIngredient, ActiveIngredientExtended, IngredientMapping, IngredientMappingLog
from claude_mapper import ClaudeMappingView

class CustomAdminIndexView(AdminIndexView):
    """Custom admin dashboard with statistics"""
    
    @expose('/')
    def index(self):
        # Get basic statistics
        stats = {
            'total_raw_ingredients': ActiveIngredient.query.count(),
            'total_extended_ingredients': ActiveIngredientExtended.query.count(),
            'total_mappings': IngredientMapping.query.count(),
            'verified_mappings': IngredientMapping.query.filter_by(verified=True).count(),
            'pending_mappings': IngredientMapping.query.filter_by(verified=False).count(),
            'high_confidence_mappings': IngredientMapping.query.filter(IngredientMapping.confidence >= 0.9).count(),
            'avg_confidence': db.session.query(db.func.avg(IngredientMapping.confidence)).scalar() or 0,
            'recent_mappings': IngredientMapping.query.order_by(IngredientMapping.created_at.desc()).limit(5).all()
        }
        
        return self.render('admin/dashboard.html', stats=stats)

class ActiveIngredientView(ModelView):
    """Admin view for raw active ingredients"""
    
    # List view configuration
    column_list = ['name', 'mappings']
    column_searchable_list = ['name']
    column_default_sort = 'name'
    
    # Details view
    column_details_list = ['ingredient_id', 'name', 'mappings']
    
    # Form configuration
    form_columns = ['name']
    
    # Formatting
    column_formatters = {
        'mappings': lambda v, c, m, p: f"{len(m.mappings)} mappings"
    }
    
    # Disable editing of raw ingredients (they come from source data)
    can_edit = False
    can_create = False
    can_delete = False
    
    # Pagination
    page_size = 25
    can_export = True

class ActiveIngredientExtendedView(ModelView):
    """Admin view for extended/clean ingredients"""
    
    # List view configuration
    column_list = ['ingredient_name', 'short_description', 'processing_status', 'last_updated', 'mappings']
    column_searchable_list = ['ingredient_name', 'short_description']
    column_filters = ['processing_status', 'last_updated']
    column_default_sort = ('last_updated', True)
    
    # Form configuration
    form_columns = ['ingredient_name', 'short_description', 'common_uses', 'side_effects', 'contraindications', 'processing_status']
    
    # Formatting
    column_formatters = {
        'short_description': lambda v, c, m, p: (m.short_description[:50] + '...') if m.short_description and len(m.short_description) > 50 else m.short_description,
        'mappings': lambda v, c, m, p: f"{len(m.mappings)} mappings",
        'last_updated': lambda v, c, m, p: m.last_updated.strftime('%Y-%m-%d %H:%M') if m.last_updated else ""
    }
    
    # Enable editing
    can_edit = True
    can_create = True
    can_delete = True
    
    page_size = 25
    can_export = True

class IngredientMappingView(ModelView):
    """Admin view for ingredient mappings with approval workflow"""
    
    # List view configuration
    column_list = [
        'raw_ingredient.name', 
        'extended_ingredient.ingredient_name', 
        'mapping_type', 
        'confidence', 
        'verified', 
        'created_by',
        'created_at'
    ]
    
    column_searchable_list = ['raw_ingredient.name', 'extended_ingredient.ingredient_name']
    column_filters = [
        'mapping_type', 
        'verified', 
        'confidence', 
        'extraction_method',
        'created_by',
        'created_at'
    ]
    
    # Add choices for better filtering
    column_choices = {
        'verified': [
            (True, 'Yes'),
            (False, 'No')
        ],
        'mapping_type': [
            ('exact', 'Exact'),
            ('fuzzy', 'Fuzzy'),
            ('ai_suggested', 'AI Suggested'),
            ('claude_interactive', 'Claude Interactive'),
            ('manual', 'Manual')
        ]
    }
    column_default_sort = ('created_at', True)
    
    # Form configuration
    form_columns = [
        'raw_ingredient', 
        'extended_ingredient', 
        'mapping_type', 
        'confidence',
        'extraction_method',
        'verified',
        'verified_by'
    ]
    
    # Labels for better UX
    column_labels = {
        'raw_ingredient.name': 'Raw Ingredient',
        'extended_ingredient.ingredient_name': 'Mapped To',
        'mapping_type': 'Type',
        'confidence': 'Confidence',
        'verified': 'Verified',
        'created_by': 'Created By',
        'created_at': 'Created'
    }
    
    # Formatting
    column_formatters = {
        'confidence': lambda v, c, m, p: f"{float(m.confidence):.2f}" if m.confidence else "N/A",
        'verified': lambda v, c, m, p: "Yes" if m.verified else "No",
        'created_at': lambda v, c, m, p: m.created_at.strftime('%Y-%m-%d %H:%M') if m.created_at else ""
    }
    
    # Enable bulk operations
    can_edit = True
    can_delete = True
    
    # Custom actions for bulk approval/rejection
    @expose('/bulk_approve', methods=['POST'])
    def bulk_approve(self):
        """Bulk approve selected mappings"""
        ids = request.form.getlist('ids')
        if ids:
            mappings = IngredientMapping.query.filter(IngredientMapping.id.in_(ids)).all()
            for mapping in mappings:
                mapping.verified = True
                mapping.verified_by = 'admin'
                mapping.verified_at = datetime.utcnow()
            
            db.session.commit()
            flash(f'Approved {len(mappings)} mappings', 'success')
        
        return redirect(url_for('.index_view'))
    
    page_size = 25
    can_export = True

class IngredientMappingLogView(ModelView):
    """Admin view for mapping audit logs"""
    
    # Read-only view
    can_create = False
    can_edit = False
    can_delete = False
    
    # List view configuration
    column_list = ['mapping_id', 'action', 'changed_by', 'changed_at']
    column_filters = ['action', 'changed_by', 'changed_at']
    column_default_sort = ('changed_at', True)
    
    # Formatting
    column_formatters = {
        'changed_at': lambda v, c, m, p: m.changed_at.strftime('%Y-%m-%d %H:%M:%S') if m.changed_at else ""
    }
    
    page_size = 50

def create_app(config_name='default'):
    """Create Flask application with admin interface"""
    
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    
    # Initialize database
    db.init_app(app)
    
    # Create admin interface
    admin = Admin(
        app, 
        name='Drug Analysis Admin',
        template_mode='bootstrap4',
        index_view=CustomAdminIndexView()
    )
    
    # Add model views
    admin.add_view(ActiveIngredientView(ActiveIngredient, db.session, name='Raw Ingredients'))
    admin.add_view(ActiveIngredientExtendedView(ActiveIngredientExtended, db.session, name='Clean Ingredients'))
    admin.add_view(IngredientMappingView(IngredientMapping, db.session, name='Mappings'))
    admin.add_view(IngredientMappingLogView(IngredientMappingLog, db.session, name='Audit Log'))
    
    # Add Claude Interactive Mapper
    admin.add_view(ClaudeMappingView(name='Claude Mapper', endpoint='claude_mapper'))
    
    # Basic routes
    @app.route('/')
    def index():
        return redirect('/admin')
    
    @app.errorhandler(404)
    def not_found(error):
        return redirect('/admin')
    
    return app

if __name__ == '__main__':
    app = create_app('development')
    
    with app.app_context():
        # Test database connection
        try:
            with db.engine.connect() as conn:
                result = conn.execute(db.text('SELECT 1'))
            print("Database connection successful")
        except Exception as e:
            print(f"Database connection failed: {e}")
            exit(1)
    
    print("🚀 Starting Drug Analysis Admin Interface...")
    print("📊 Dashboard: http://localhost:5000/admin")
    print("🔍 Raw Ingredients: http://localhost:5000/admin/activeingredient/")
    print("✨ Clean Ingredients: http://localhost:5000/admin/activeingredientextended/")
    print("🔗 Mappings: http://localhost:5000/admin/ingredientmapping/")
    
    app.run(debug=True, host='0.0.0.0', port=5000)