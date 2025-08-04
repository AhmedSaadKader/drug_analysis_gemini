# Drug Analysis Admin Interface

A professional Flask-Admin interface for managing pharmaceutical ingredient mappings.

## 🚀 Quick Start

### 1. Install Dependencies
```bash
# From project root
pip install -r requirements.txt
```

### 2. Configure Environment
```bash
# Ensure .env file exists with database credentials
cp .env.example .env
# Edit .env with your database details
```

### 3. Start the Admin Interface
```bash
cd admin
python run.py
```

### 4. Access the Interface
- **Main Dashboard**: http://localhost:5000/admin
- **Raw Ingredients**: http://localhost:5000/admin/activeingredient/
- **Clean Ingredients**: http://localhost:5000/admin/activeingredientextended/
- **Mappings**: http://localhost:5000/admin/ingredientmapping/
- **Audit Log**: http://localhost:5000/admin/ingredientmappinglog/

## 📊 Features

### Dashboard
- **Real-time Statistics** - Total ingredients, mappings, confidence scores
- **Quality Metrics** - Verification rates and pending approvals
- **Recent Activity** - Latest mappings with confidence indicators
- **Quick Actions** - Direct links to common tasks

### Raw Ingredients Management
- **Browse & Search** - Search through 7,800+ raw ingredient names
- **Export Data** - CSV export for external analysis
- **View Mappings** - See which clean ingredients each raw ingredient maps to

### Clean Ingredients Management
- **Professional Interface** - Manage standardized ingredient database
- **Full CRUD Operations** - Create, edit, delete clean ingredients
- **Rich Descriptions** - Add descriptions and common uses
- **Status Tracking** - Track processing status of ingredients

### Mapping Management
- **Approval Workflow** - Review and approve AI-generated mappings
- **Confidence Filtering** - Filter by confidence scores
- **Bulk Operations** - Approve multiple mappings at once
- **Search & Filter** - Find specific mappings quickly
- **Audit Trail** - Complete history of mapping changes

## 🎯 Common Workflows

### Reviewing Claude Mappings
1. Go to **Mappings** section
2. Filter by `verified = False` to see pending mappings
3. Review mapping quality and confidence scores
4. Use bulk approve for high-confidence mappings
5. Edit individual mappings if needed

### Adding New Clean Ingredients
1. Go to **Clean Ingredients** section
2. Click **Create** to add new ingredient
3. Fill in name, description, and common uses
4. Set status to 'completed'
5. Save the new ingredient

### Quality Analysis
1. Check **Dashboard** for overall statistics
2. Use **Audit Log** to track changes over time
3. Export data from any section for external analysis
4. Monitor confidence scores and verification rates

## 🔧 Technical Details

### Architecture
- **Flask** - Web framework
- **Flask-Admin** - Admin interface
- **SQLAlchemy** - Database ORM
- **PostgreSQL** - Database backend
- **Bootstrap 4** - UI framework

### Database Models
- **ActiveIngredient** - Raw ingredient data (7,800+ records)
- **ActiveIngredientExtended** - Clean ingredients (3,400+ records)
- **IngredientMapping** - Many-to-many mappings (380+ records)
- **IngredientMappingLog** - Audit trail for all changes

### Security Features
- **Input Validation** - All forms validated
- **SQL Injection Protection** - SQLAlchemy ORM protection
- **CSRF Protection** - Flask-WTF CSRF tokens
- **Audit Logging** - Complete change history

## 🛠️ Customization

### Adding Custom Views
```python
# In admin/views/custom_views.py
from flask_admin import BaseView, expose

class CustomAnalysisView(BaseView):
    @expose('/')
    def index(self):
        # Custom analysis logic
        return self.render('custom_analysis.html')

# Add to admin/app.py
admin.add_view(CustomAnalysisView(name='Analysis'))
```

### Custom Actions
```python
# In model views
@action('custom_action', 'Custom Action', 'Confirmation message')
def action_custom(self, ids):
    # Custom bulk action logic
    pass
```

## 🔍 Troubleshooting

### Database Connection Issues
```bash
# Test database connection
python -c "
import os
from dotenv import load_dotenv
load_dotenv()
import psycopg2
conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    database=os.getenv('DB_NAME')
)
print('✅ Database connection successful')
conn.close()
"
```

### Common Issues
1. **Import Errors**: Run from admin/ directory
2. **Database Not Found**: Check .env configuration
3. **Permission Denied**: Ensure PostgreSQL is running
4. **Port Already in Use**: Change port in run.py

### Performance Tips
- **Pagination**: Default 25 items per page
- **Filtering**: Use filters for large datasets
- **Indexing**: Ensure database indexes are optimal
- **Caching**: Enable Flask caching in production

## 🚀 Production Deployment

### Using Gunicorn
```bash
pip install gunicorn
gunicorn --bind 0.0.0.0:5000 --workers 4 app:create_app()
```

### Using Docker
```dockerfile
FROM python:3.9
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
EXPOSE 5000
CMD ["python", "admin/run.py"]
```

### Environment Variables
- `FLASK_ENV=production`
- `FLASK_SECRET_KEY=your-secret-key`
- Database credentials as usual