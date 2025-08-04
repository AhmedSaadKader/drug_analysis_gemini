# Scripts Directory

This directory contains all executable scripts for the Drug Analysis System.

## Directory Structure

### 📁 `production/` - Ready-to-Use Scripts
These scripts are production-ready and actively maintained:

- **`claude_interactive_mapper.py`** 🌟
  - **Primary ingredient mapping tool**
  - Uses Claude AI for high-accuracy mapping
  - Interactive workflow with human review
  - Usage: `python scripts/production/claude_interactive_mapper.py --sample 20`

- **`ingredient_mapping_processor.py`**
  - Gemini AI-based ingredient processor
  - Automated batch processing
  - Legacy tool (use Claude mapper for better results)
  - Usage: `python -m scripts.production.ingredient_mapping_processor --sample 100`

- **`pharmaceutical_category_linker.py`**
  - Categorizes ingredients into pharmaceutical classes
  - Uses AI for intelligent classification
  - Usage: `python scripts/production/pharmaceutical_category_linker.py --sample 50`

- **`backup.py`**
  - Production-grade database backup utility
  - Automated compression and verification
  - Usage: `python scripts/production/backup.py`

- **`db_analyzer.py`**
  - Comprehensive database analysis tool
  - Generates detailed reports in JSON format
  - Usage: `python scripts/production/db_analyzer.py`

- **`ingredient_quality_analyzer.py`**
  - Analyzes mapping quality and generates reports
  - Identifies patterns in rejected/accepted mappings
  - Usage: `python scripts/production/ingredient_quality_analyzer.py`

### 📁 `legacy/` - Deprecated Scripts
These scripts are kept for reference but are no longer actively used:

- **`delete_duplicate.py`** - Basic duplicate removal (superseded)
- **`advanced_duplicate_cleanup.py`** - Complex duplicate handling (potentially obsolete)
- **`drug_ingredient_linker.py`** - Original mapping tool (superseded by mapping processor)
- **`database.py`** - Basic database wrapper (functionality absorbed)

### 📄 SQL Files
- **`create_ingredient_mapping_tables.sql`** - Database schema for mapping system
- **`drug_ingredient_query.sql`** - Utility queries for data exploration

## Usage Guidelines

### Running Production Scripts

1. **Always test with samples first**:
```bash
python scripts/production/claude_interactive_mapper.py --sample 10
```

2. **Monitor logs during execution**:
```bash
tail -f logs/claudeinteractivemapper_*.log
```

3. **Use appropriate batch sizes**:
   - Small batches (10-20): Testing and quality validation
   - Medium batches (50-100): Regular processing
   - Large batches (100+): Production runs

### Environment Requirements

All production scripts require:
- ✅ Valid `.env` file with database and API credentials
- ✅ PostgreSQL database connection
- ✅ Google Gemini API access (for AI-powered scripts)
- ✅ Python dependencies from `requirements.txt`

### Logging

All scripts generate timestamped logs in the `logs/` directory:
- Main operation logs: `{script_name}_{timestamp}.log`
- Change tracking logs: `{operation}_changes_{timestamp}.log`

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure you're running from the project root directory
2. **Database Connection**: Verify PostgreSQL service and `.env` credentials
3. **API Rate Limits**: Scripts handle Gemini API limits automatically
4. **Permission Errors**: Ensure write access to `logs/` directory

### Performance Tips

- **Memory Usage**: Reduce batch sizes for large datasets
- **API Costs**: Monitor Gemini API usage in Google Cloud Console
- **Database Performance**: Regular VACUUM and ANALYZE operations

## Migration from Old Structure

If you have old commands, update them as follows:

**Old**:
```bash
python scripts/claude_interactive_mapper.py --sample 20
```

**New**:
```bash
python scripts/production/claude_interactive_mapper.py --sample 20
```

## Future Development

New scripts should be added to:
- `production/` - For production-ready tools
- `legacy/` - For deprecated/experimental scripts (rarely used)
- Root `scripts/` - Only for SQL files and documentation