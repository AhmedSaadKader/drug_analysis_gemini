-- =====================================================
-- Ingredient Mapping System - Many-to-Many Structure
-- =====================================================
-- This script creates the proper many-to-many mapping system
-- to link messy active_ingredients to clean active_ingredients_extended

-- Drop existing tables if they exist (for clean setup)
DROP TABLE IF EXISTS ingredient_mappings CASCADE;
DROP TABLE IF EXISTS ingredient_mapping_log CASCADE;

-- =====================================================
-- Main Many-to-Many Mapping Table
-- =====================================================
CREATE TABLE ingredient_mappings (
    id SERIAL PRIMARY KEY,
    
    -- Foreign Keys
    raw_ingredient_id UUID NOT NULL REFERENCES active_ingredients(ingredient_id),
    extended_ingredient_id INTEGER NOT NULL REFERENCES active_ingredients_extended(id),
    
    -- Mapping Classification
    mapping_type VARCHAR(50) NOT NULL CHECK (mapping_type IN (
        'exact',           -- Perfect match
        'fuzzy',           -- Close match (typos, formatting)
        'ai_suggested',    -- AI-generated mapping
        'partial_match',   -- Part of compound ingredient
        'manual',          -- Manually verified/corrected
        'synonym'          -- Known pharmaceutical synonym
    )),
    
    -- Quality Metrics
    confidence DECIMAL(3,2) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    extraction_method VARCHAR(50) CHECK (extraction_method IN (
        'direct_match',    -- Direct 1:1 mapping
        'split_compound',  -- Extracted from compound ingredient
        'typo_fix',        -- Corrected spelling/formatting
        'synonym_match',   -- Alternative name match
        'ai_analysis',     -- AI-generated suggestion
        'manual_entry'     -- Human-entered mapping
    )),
    
    -- Detailed Information
    original_text TEXT,              -- The exact part that matched (for compounds)
    ai_notes TEXT,                   -- AI analysis details
    similarity_score DECIMAL(3,2),   -- Text similarity metric (if applicable)
    
    -- Quality Control & Audit
    verified BOOLEAN DEFAULT FALSE,
    verified_by VARCHAR(100),
    verified_at TIMESTAMP,
    verification_notes TEXT,
    
    -- System Fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'system',
    
    -- Constraints
    UNIQUE(raw_ingredient_id, extended_ingredient_id),
    CHECK (confidence > 0.5 OR verified = TRUE)  -- Low confidence requires verification
);

-- =====================================================
-- Audit/History Table for Mapping Changes
-- =====================================================
CREATE TABLE ingredient_mapping_log (
    log_id SERIAL PRIMARY KEY,
    mapping_id INTEGER REFERENCES ingredient_mappings(id),
    action VARCHAR(20) NOT NULL CHECK (action IN ('INSERT', 'UPDATE', 'DELETE', 'VERIFY')),
    old_values JSONB,
    new_values JSONB,
    changed_by VARCHAR(100),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reason TEXT
);

-- =====================================================
-- Indexes for Performance
-- =====================================================
-- Primary lookup indexes
CREATE INDEX idx_ingredient_mappings_raw_id ON ingredient_mappings(raw_ingredient_id);
CREATE INDEX idx_ingredient_mappings_extended_id ON ingredient_mappings(extended_ingredient_id);

-- Query optimization indexes
CREATE INDEX idx_ingredient_mappings_confidence ON ingredient_mappings(confidence DESC);
CREATE INDEX idx_ingredient_mappings_type ON ingredient_mappings(mapping_type);
CREATE INDEX idx_ingredient_mappings_verified ON ingredient_mappings(verified);
CREATE INDEX idx_ingredient_mappings_method ON ingredient_mappings(extraction_method);

-- Composite indexes for common queries
CREATE INDEX idx_ingredient_mappings_type_confidence ON ingredient_mappings(mapping_type, confidence DESC);
CREATE INDEX idx_ingredient_mappings_verified_confidence ON ingredient_mappings(verified, confidence DESC);

-- Text search index for notes
CREATE INDEX idx_ingredient_mappings_ai_notes_gin ON ingredient_mappings USING gin(to_tsvector('english', ai_notes));

-- =====================================================
-- Audit Trigger Function
-- =====================================================
CREATE OR REPLACE FUNCTION audit_ingredient_mappings()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO ingredient_mapping_log (mapping_id, action, new_values, changed_by)
        VALUES (NEW.id, 'INSERT', to_jsonb(NEW), NEW.created_by);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO ingredient_mapping_log (mapping_id, action, old_values, new_values, changed_by)
        VALUES (NEW.id, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW), COALESCE(NEW.verified_by, 'system'));
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO ingredient_mapping_log (mapping_id, action, old_values, changed_by)
        VALUES (OLD.id, 'DELETE', to_jsonb(OLD), 'system');
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create audit trigger
CREATE TRIGGER trigger_audit_ingredient_mappings
    AFTER INSERT OR UPDATE OR DELETE ON ingredient_mappings
    FOR EACH ROW EXECUTE FUNCTION audit_ingredient_mappings();

-- =====================================================
-- Helper Views for Common Queries
-- =====================================================

-- View: Complete ingredient mapping with names
CREATE OR REPLACE VIEW ingredient_mapping_details AS
SELECT 
    im.id,
    im.raw_ingredient_id,
    ai.name as raw_ingredient_name,
    im.extended_ingredient_id,
    aie.ingredient_name as extended_ingredient_name,
    aie.short_description,
    im.mapping_type,
    im.confidence,
    im.extraction_method,
    im.original_text,
    im.ai_notes,
    im.verified,
    im.verified_by,
    im.verified_at,
    im.created_at
FROM ingredient_mappings im
JOIN active_ingredients ai ON im.raw_ingredient_id = ai.ingredient_id
JOIN active_ingredients_extended aie ON im.extended_ingredient_id = aie.id;

-- View: Compound ingredients (1 raw -> multiple extended)
CREATE OR REPLACE VIEW compound_ingredient_mappings AS
SELECT 
    ai.ingredient_id,
    ai.name as raw_ingredient,
    COUNT(im.extended_ingredient_id) as component_count,
    STRING_AGG(aie.ingredient_name, ' + ' ORDER BY aie.ingredient_name) as components,
    AVG(im.confidence) as avg_confidence,
    MIN(im.confidence) as min_confidence
FROM active_ingredients ai
JOIN ingredient_mappings im ON ai.ingredient_id = im.raw_ingredient_id
JOIN active_ingredients_extended aie ON im.extended_ingredient_id = aie.id
GROUP BY ai.ingredient_id, ai.name
HAVING COUNT(im.extended_ingredient_id) > 1;

-- View: Quality control - needs verification
CREATE OR REPLACE VIEW mappings_need_verification AS
SELECT 
    im.id,
    ai.name as raw_ingredient,
    aie.ingredient_name as extended_ingredient,
    im.confidence,
    im.mapping_type,
    im.ai_notes,
    im.created_at
FROM ingredient_mappings im
JOIN active_ingredients ai ON im.raw_ingredient_id = ai.ingredient_id
JOIN active_ingredients_extended aie ON im.extended_ingredient_id = aie.id
WHERE im.verified = FALSE 
  AND (im.confidence < 0.8 OR im.mapping_type = 'ai_suggested')
ORDER BY im.confidence ASC, im.created_at DESC;

-- View: Drug-ingredient links through mappings
CREATE OR REPLACE VIEW drug_ingredient_links AS
SELECT DISTINCT
    dd.drug_id,
    dd.tradename,
    dd.activeingredient as raw_drug_ingredients,
    ai.name as matched_raw_ingredient,
    aie.ingredient_name as clean_ingredient,
    aie.short_description as ingredient_description,
    im.confidence,
    im.mapping_type,
    im.verified
FROM drug_database dd
JOIN active_ingredients ai ON dd.activeingredient ILIKE '%' || ai.name || '%'
JOIN ingredient_mappings im ON ai.ingredient_id = im.raw_ingredient_id
JOIN active_ingredients_extended aie ON im.extended_ingredient_id = aie.id
WHERE im.confidence > 0.7;

-- =====================================================
-- Statistics View
-- =====================================================
CREATE OR REPLACE VIEW mapping_statistics AS
SELECT 
    'Total Mappings' as metric,
    COUNT(*)::TEXT as value
FROM ingredient_mappings
UNION ALL
SELECT 
    'Verified Mappings',
    COUNT(*)::TEXT
FROM ingredient_mappings WHERE verified = TRUE
UNION ALL
SELECT 
    'High Confidence (>0.9)',
    COUNT(*)::TEXT
FROM ingredient_mappings WHERE confidence > 0.9
UNION ALL
SELECT 
    'Compound Ingredients',
    COUNT(DISTINCT raw_ingredient_id)::TEXT
FROM ingredient_mappings
GROUP BY raw_ingredient_id
HAVING COUNT(*) > 1
UNION ALL
SELECT 
    'Coverage %',
    ROUND(
        (COUNT(DISTINCT im.raw_ingredient_id)::DECIMAL / 
         (SELECT COUNT(*) FROM active_ingredients)) * 100, 1
    )::TEXT || '%'
FROM ingredient_mappings im;

-- =====================================================
-- Sample Data Insertion Function
-- =====================================================
CREATE OR REPLACE FUNCTION insert_sample_mappings()
RETURNS TEXT AS $$
DECLARE
    result_text TEXT := 'Sample mappings inserted successfully';
BEGIN
    -- This function can be used to insert test data
    -- Will be populated by the Python script
    RETURN result_text;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Utility Functions
-- =====================================================

-- Function to get mapping statistics
CREATE OR REPLACE FUNCTION get_mapping_stats()
RETURNS TABLE(
    total_mappings BIGINT,
    verified_mappings BIGINT,
    high_confidence_mappings BIGINT,
    compound_ingredients BIGINT,
    avg_confidence DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) as total_mappings,
        COUNT(*) FILTER (WHERE verified = TRUE) as verified_mappings,
        COUNT(*) FILTER (WHERE confidence > 0.9) as high_confidence_mappings,
        COUNT(DISTINCT raw_ingredient_id) FILTER (
            WHERE raw_ingredient_id IN (
                SELECT raw_ingredient_id 
                FROM ingredient_mappings 
                GROUP BY raw_ingredient_id 
                HAVING COUNT(*) > 1
            )
        ) as compound_ingredients,
        ROUND(AVG(confidence), 3) as avg_confidence
    FROM ingredient_mappings;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Data Validation Functions
-- =====================================================

-- Function to validate mapping integrity
CREATE OR REPLACE FUNCTION validate_mappings()
RETURNS TABLE(
    issue_type TEXT,
    issue_count BIGINT,
    description TEXT
) AS $$
BEGIN
    RETURN QUERY
    -- Check for orphaned raw ingredients
    SELECT 
        'Orphaned Raw Ingredients'::TEXT,
        COUNT(*)::BIGINT,
        'Raw ingredients that don''t exist in active_ingredients table'::TEXT
    FROM ingredient_mappings im
    LEFT JOIN active_ingredients ai ON im.raw_ingredient_id = ai.ingredient_id
    WHERE ai.ingredient_id IS NULL
    
    UNION ALL
    
    -- Check for orphaned extended ingredients
    SELECT 
        'Orphaned Extended Ingredients'::TEXT,
        COUNT(*)::BIGINT,
        'Extended ingredients that don''t exist in active_ingredients_extended table'::TEXT
    FROM ingredient_mappings im
    LEFT JOIN active_ingredients_extended aie ON im.extended_ingredient_id = aie.id
    WHERE aie.id IS NULL
    
    UNION ALL
    
    -- Check for low confidence unverified mappings
    SELECT 
        'Low Confidence Unverified'::TEXT,
        COUNT(*)::BIGINT,
        'Mappings with confidence < 0.8 that need verification'::TEXT
    FROM ingredient_mappings
    WHERE confidence < 0.8 AND verified = FALSE;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Success Message
-- =====================================================
DO $$
BEGIN
    RAISE NOTICE '✅ Ingredient mapping system created successfully!';
    RAISE NOTICE '📊 Tables created: ingredient_mappings, ingredient_mapping_log';
    RAISE NOTICE '👁️  Views created: ingredient_mapping_details, compound_ingredient_mappings, mappings_need_verification, drug_ingredient_links, mapping_statistics';
    RAISE NOTICE '🔧 Functions created: get_mapping_stats(), validate_mappings()';
    RAISE NOTICE '📝 Next step: Run the Python script to populate mappings with AI analysis';
END;
$$;