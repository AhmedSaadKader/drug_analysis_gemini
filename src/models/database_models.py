"""
Database Models for Drug Analysis System
Defines the structure of database tables for SQLAlchemy integration
"""

from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import uuid

db = SQLAlchemy()

class ActiveIngredient(db.Model):
    """Raw ingredient data from original dataset"""
    __tablename__ = 'active_ingredients'
    
    ingredient_id = db.Column(db.String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = db.Column(db.String, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    mappings = db.relationship('IngredientMapping', backref='raw_ingredient', lazy=True)

class ActiveIngredientExtended(db.Model):
    """Clean, standardized ingredient data"""
    __tablename__ = 'active_ingredients_extended'
    
    id = db.Column(db.Integer, primary_key=True)
    ingredient_name = db.Column(db.String, nullable=False, unique=True)
    short_description = db.Column(db.Text)
    common_uses = db.Column(db.Text)
    processing_status = db.Column(db.String, default='pending')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    mappings = db.relationship('IngredientMapping', backref='extended_ingredient', lazy=True)

class IngredientMapping(db.Model):
    """Many-to-many mapping between raw and extended ingredients"""
    __tablename__ = 'ingredient_mappings'
    
    id = db.Column(db.Integer, primary_key=True)
    raw_ingredient_id = db.Column(db.String, db.ForeignKey('active_ingredients.ingredient_id'), nullable=False)
    extended_ingredient_id = db.Column(db.Integer, db.ForeignKey('active_ingredients_extended.id'), nullable=False)
    mapping_type = db.Column(db.String(50), nullable=False)
    confidence = db.Column(db.Numeric(3,2), nullable=False)
    extraction_method = db.Column(db.String(50))
    verified = db.Column(db.Boolean, default=False)
    verified_by = db.Column(db.String(100))
    verified_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    created_by = db.Column(db.String(100), default='system')
    
    # Constraints
    __table_args__ = (
        db.UniqueConstraint('raw_ingredient_id', 'extended_ingredient_id'),
        db.CheckConstraint('confidence >= 0 AND confidence <= 1'),
    )

class IngredientMappingLog(db.Model):
    """Audit log for ingredient mapping changes"""
    __tablename__ = 'ingredient_mapping_log'
    
    id = db.Column(db.Integer, primary_key=True)
    mapping_id = db.Column(db.Integer, db.ForeignKey('ingredient_mappings.id'))
    action = db.Column(db.String(20), nullable=False)  # INSERT, UPDATE, DELETE
    old_values = db.Column(db.JSON)
    new_values = db.Column(db.JSON)
    changed_by = db.Column(db.String(100), default='system')
    changed_at = db.Column(db.DateTime, default=datetime.utcnow)