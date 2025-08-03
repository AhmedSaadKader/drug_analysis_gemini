# Claude Code Assistant Guide

This document provides guidance for Claude Code when working with the Drug Analysis Gemini project.

## Project Overview

This is a pharmaceutical database analysis system that uses Google's Gemini AI for intelligent drug-ingredient mapping and pharmaceutical categorization. The system processes large datasets and requires careful handling of API rate limits and database operations.

## Key System Information

### Database Configuration
- **Database Name**: `pharmacy_db`
- **Default User**: `postgres`
- **Host**: `localhost`
- **Password**: Stored in `.env` file as `DB_PASSWORD`

### API Configuration
- **Gemini API**: Google Gemini 2.0 Flash model
- **Rate Limit**: 15 requests per minute (4-second delays implemented)
- **API Key**: Stored in `.env` file as `GOOGLE_API_KEY`

## Script Classifications

### Production-Ready Scripts (Keep)
1. **`scripts/pharmaceutical_category_linker.py`** - ⭐ PRIMARY TOOL
   - Most recent and well-structured
   - Comprehensive logging and error handling
   - Handles pharmaceutical category classification
   - Command: `python scripts/pharmaceutical_category_linker.py --sample 50`

2. **`scripts/drug_ingredient_linker.py`** - ⭐ PRIMARY TOOL
   - Complex drug-ingredient mapping with AI
   - Advanced text cleaning and validation
   - Batch processing with rate limiting
   - Command: `python scripts/drug_ingredient_linker.py --sample 100`

3. **`scripts/backup.py`** - ✅ PRODUCTION UTILITY
   - Production-grade database backup system
   - Verification and cleanup features
   - Uses `utils/logger_setup.py` properly
   - Command: `python scripts/backup.py`

4. **`utils/logger_setup.py`** - ✅ CORE UTILITY
   - Reusable logging configuration class
   - Used by newer scripts
   - Well-structured and documented

5. **`scripts/db_analyzer.py`** - ✅ ANALYSIS TOOL
   - Comprehensive database analysis
   - JSON output format
   - Good for understanding database structure
   - Command: `python scripts/db_analyzer.py`

### Legacy/Deprecated Scripts (Consider Removing)
1. **`scripts/delete_duplicate.py`** - ❌ LEGACY
   - Basic duplicate cleanup functionality
   - Superseded by advanced version
   - Last modified: Feb 12

2. **`scripts/advanced_duplicate_cleanup.py`** - ❌ POTENTIALLY OBSOLETE
   - Complex duplicate handling with AI
   - Appears to have integration issues
   - Last modified: Feb 13 (early development)

3. **`database.py`** - ❌ DEPRECATED
   - Simple database wrapper
   - Functionality absorbed into other scripts
   - Last modified: Feb 11

4. **`__init__.py` files** - ❌ EMPTY PLACEHOLDERS
   - Empty files, no functionality
   - Can be removed or properly structured

### Core Configuration Files (Keep)
1. **`config.py`** - ✅ ESSENTIAL
   - Environment variable management
   - Database and API configuration
   - Error handling for missing variables

2. **`gemini_api.py`** - ✅ ESSENTIAL
   - Gemini API integration
   - Used by main processing scripts
   - Simple but functional

## Common Operations

### Running Tests/Samples
```bash
# Test pharmaceutical categorization
python scripts/pharmaceutical_category_linker.py --sample 25 --batch-size 10

# Test drug-ingredient linking
python scripts/drug_ingredient_linker.py --sample 50 --batch-size 15

# Analyze database structure
python scripts/db_analyzer.py
```

### Production Operations
```bash
# Full pharmaceutical categorization
python scripts/pharmaceutical_category_linker.py --batch-size 25

# Full drug-ingredient linking
python scripts/drug_ingredient_linker.py --batch-size 25 --auto-confirm

# Database backup
python scripts/backup.py
```

### Log File Locations
- Main logs: `logs/{script_name}_{timestamp}.log`
- Change logs: `logs/{operation}_changes_{timestamp}.log`
- All logs use timestamps: `YYYYMMDD_HHMMSS`

## Development Guidelines

### When Working with This Project

1. **Rate Limiting**: Always respect Gemini API limits (15 RPM)
2. **Batch Processing**: Use appropriate batch sizes (25-50 items)
3. **Logging**: Use the `LoggerSetup` utility for consistent logging
4. **Testing**: Always use `--sample` for testing before full runs
5. **Database**: Be cautious with direct database modifications

### Code Quality Standards
- Follow existing logging patterns
- Implement proper error handling
- Add command-line arguments for flexibility
- Include progress tracking for long operations
- Validate AI responses before database updates

### File Operations
- Main scripts are in `scripts/` directory
- Utilities are in `utils/` directory
- Configuration files are in root directory
- Logs are generated in `logs/` directory
- Backups are stored in `backups/` directory

## Troubleshooting

### Common Issues
1. **Database Connection Errors**: Check PostgreSQL service and `.env` file
2. **API Rate Limits**: Scripts handle this automatically with delays
3. **Memory Issues**: Reduce batch sizes for large datasets
4. **Permission Errors**: Ensure write access to `logs/` and `backups/` directories

### Performance Considerations
- Gemini API calls are the bottleneck (4-second delays)
- Database operations are generally fast for reasonable batch sizes
- Log files can grow large during full processing runs
- Consider regular cleanup of old log files and backups

## Recommended Actions for Cleanup

### Scripts to Remove
1. `scripts/delete_duplicate.py` - Superseded functionality
2. `scripts/advanced_duplicate_cleanup.py` - Appears obsolete
3. `database.py` - Basic wrapper, no longer needed
4. Empty `__init__.py` files (unless needed for package structure)

### Scripts to Keep and Maintain
1. `scripts/pharmaceutical_category_linker.py` - Primary categorization tool
2. `scripts/drug_ingredient_linker.py` - Primary mapping tool
3. `scripts/backup.py` - Production backup system
4. `scripts/db_analyzer.py` - Database analysis utility
5. `utils/logger_setup.py` - Core logging utility
6. `config.py` - Configuration management
7. `gemini_api.py` - API integration

### Testing Commands for Validation
```bash
# Test core functionality
python scripts/pharmaceutical_category_linker.py --sample 5
python scripts/drug_ingredient_linker.py --sample 5
python scripts/backup.py
python scripts/db_analyzer.py
```

This should help you understand the system architecture and make informed decisions about script usage and maintenance.