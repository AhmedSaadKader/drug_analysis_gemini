#!/usr/bin/env python3
"""
Simple startup script for Drug Analysis Admin Interface
"""
import sys
import os

# Add current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import create_app

if __name__ == '__main__':
    # Create the Flask app
    app = create_app('development')
    
    # Test database connection before starting
    with app.app_context():
        try:
            from models import db
            # Simple connection test
            with db.engine.connect() as conn:
                result = conn.execute(db.text('SELECT COUNT(*) FROM active_ingredients'))
                count = result.fetchone()[0]
            print(f"Database connection successful - Found {count:,} raw ingredients")
        except Exception as e:
            print(f"Database connection failed: {e}")
            print("Please check your .env file and PostgreSQL connection")
            sys.exit(1)
    
    print("\n" + "="*60)
    print("DRUG ANALYSIS ADMIN INTERFACE STARTING")
    print("="*60)
    print("Main Dashboard:    http://localhost:5000/admin")
    print("Raw Ingredients:   http://localhost:5000/admin/activeingredient/")
    print("Clean Ingredients: http://localhost:5000/admin/activeingredientextended/")
    print("Mappings:         http://localhost:5000/admin/ingredientmapping/")
    print("Audit Log:        http://localhost:5000/admin/ingredientmappinglog/")
    print("="*60)
    print("Press Ctrl+C to stop the server")
    print("="*60 + "\n")
    
    # Start the development server
    app.run(
        debug=True, 
        host='0.0.0.0', 
        port=5000,
        use_reloader=True
    )