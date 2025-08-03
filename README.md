# Drug Analysis Gemini

A comprehensive pharmaceutical database analysis system leveraging Google's Gemini AI for intelligent drug-ingredient mapping and categorization.

## Overview

This project provides a suite of tools for analyzing pharmaceutical databases, with a focus on:
- Intelligent drug-ingredient linking using AI
- Pharmaceutical category classification
- Database analysis and maintenance
- Automated backup and verification systems

## System Architecture

### Core Components

1. **AI Integration** (`gemini_api.py`)
   - Google Gemini 2.0 Flash model integration
   - Content generation for pharmaceutical analysis
   - Token usage tracking

2. **Configuration Management** (`config.py`)
   - Environment-based configuration
   - Database connection parameters
   - API key management

3. **Logging System** (`utils/logger_setup.py`)
   - Standardized logging across all modules
   - Dual file/console output
   - Timestamped log files

### Database Schema

The system works with several key tables:
- `drug_database` - Main drug information
- `active_ingredients_extended` - Ingredient master data
- `drug_ingredients` - Drug-ingredient relationships
- `pharmaceutical_categories` - Category hierarchy
- `pharmaceutical_category_relations` - Ingredient-category mappings

## Key Features

### 1. Drug-Ingredient Linking (`scripts/drug_ingredient_linker.py`)

**Purpose**: Intelligently map drugs to their active ingredients using AI analysis.

**Features**:
- Batch processing with rate limiting (15 RPM)
- Advanced ingredient text cleaning and normalization
- Pharmaceutical naming convention handling (salts, variants)
- Confidence scoring and validation
- Comprehensive mapping reports
- Support for sampling and testing modes

**Usage**:
```bash
python scripts/drug_ingredient_linker.py --sample 100 --batch-size 25
python scripts/drug_ingredient_linker.py --auto-confirm
```

### 2. Pharmaceutical Categorization (`scripts/pharmaceutical_category_linker.py`)

**Purpose**: Classify active ingredients into pharmaceutical categories using AI.

**Features**:
- Hierarchical category support
- Multi-category assignment per ingredient
- Confidence thresholds (minimum 0.85)
- Detailed categorization reasoning
- Comprehensive change logging

**Usage**:
```bash
python scripts/pharmaceutical_category_linker.py --sample 50
python scripts/pharmaceutical_category_linker.py --batch-size 25
```

### 3. Database Backup System (`scripts/backup.py`)

**Purpose**: Production-ready database backup with verification.

**Features**:
- Compressed PostgreSQL dumps
- Backup verification using pg_restore
- Metadata manifest generation
- Automated cleanup of old backups
- Comprehensive logging

**Usage**:
```bash
python scripts/backup.py
```

### 4. Database Analysis (`scripts/db_analyzer.py`)

**Purpose**: Comprehensive database structure and content analysis.

**Features**:
- Complete schema documentation
- Table statistics and sizing
- Index and constraint analysis
- Sample data extraction
- JSON output format

**Usage**:
```bash
python scripts/db_analyzer.py
```

## Installation & Setup

### Prerequisites

- Python 3.8+
- PostgreSQL database
- Google Gemini API access

### Environment Variables

Create a `.env` file with:
```
DB_PASSWORD=your_postgres_password
GOOGLE_API_KEY=your_gemini_api_key
```

### Dependencies

```bash
pip install psycopg2-binary google-generativeai python-dotenv
```

## Database Configuration

Default connection parameters:
- Database: `pharmacy_db`
- User: `postgres`
- Host: `localhost`
- Password: From environment variable

## Logging

All scripts generate timestamped logs in the `logs/` directory:
- Main operation logs: `{script_name}_{timestamp}.log`
- Change tracking logs: `{operation}_changes_{timestamp}.log`

## Best Practices

### Rate Limiting
- Gemini API: 15 requests per minute
- All AI-powered scripts implement 4-second delays

### Batch Processing
- Default batch size: 25-50 items
- Configurable via command-line arguments
- Progress tracking and error handling

### Data Validation
- Confidence thresholds for AI decisions
- Input validation and sanitization
- Comprehensive error logging

## Development Notes

### Script Status Assessment

**Production Ready**:
- `pharmaceutical_category_linker.py` - Well-structured, comprehensive
- `drug_ingredient_linker.py` - Advanced features, good error handling
- `backup.py` - Production-grade backup solution
- `logger_setup.py` - Reusable utility class

**Legacy/Deprecated**:
- `delete_duplicate.py` - Basic functionality, likely superseded
- `advanced_duplicate_cleanup.py` - Complex but potentially obsolete
- `database.py` - Simple wrapper, functionality absorbed elsewhere

### Future Improvements

1. **Configuration Management**: Centralize all configuration
2. **Testing Framework**: Add unit and integration tests
3. **Documentation**: API documentation and usage examples
4. **Monitoring**: Add performance metrics and health checks
5. **Error Recovery**: Implement retry mechanisms and graceful failures

## Troubleshooting

### Common Issues

1. **Gemini API Rate Limits**: Scripts automatically handle rate limiting
2. **Database Connection**: Verify PostgreSQL service and credentials
3. **Memory Usage**: Large datasets may require batch size adjustment
4. **Log File Permissions**: Ensure write access to logs directory

### Performance Optimization

- Adjust batch sizes based on available memory
- Monitor API usage and costs
- Regular database maintenance and indexing
- Log file rotation and cleanup

## Contributing

When adding new features:
1. Follow the established logging patterns
2. Implement proper error handling
3. Add configuration options where appropriate
4. Update documentation
5. Test with sample data first

## License

[Add your license information here]