# Setup Guide - Drug Analysis System

## Complete Installation Instructions

### 1. System Requirements

- **Operating System**: Windows 10+, macOS 10.15+, or Linux
- **Python**: 3.8 or higher
- **PostgreSQL**: 12.0 or higher
- **Memory**: 4GB RAM minimum (8GB recommended for large batches)
- **Storage**: 2GB free space for logs and backups

### 2. Database Setup

#### PostgreSQL Installation
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install postgresql postgresql-contrib

# macOS (via Homebrew)
brew install postgresql

# Windows
# Download installer from https://www.postgresql.org/download/windows/
```

#### Database Creation
```sql
-- Connect to PostgreSQL as superuser
psql -U postgres

-- Create database
CREATE DATABASE pharmacy_db;

-- Create user (optional)
CREATE USER drug_analyst WITH PASSWORD 'secure_password';
GRANT ALL PRIVILEGES ON DATABASE pharmacy_db TO drug_analyst;
```

### 3. Environment Configuration

#### Copy Environment Template
```bash
cp .env.example .env
```

#### Edit .env File
```bash
# Database Configuration
DB_HOST=localhost
DB_USER=postgres
DB_PASSWORD=your_actual_password
DB_NAME=pharmacy_db

# Google Gemini API Configuration
GOOGLE_API_KEY=your_gemini_api_key

# Optional: Logging Configuration
LOG_LEVEL=INFO
DEFAULT_BATCH_SIZE=25
```

### 4. Python Environment Setup

#### Using Virtual Environment (Recommended)
```bash
# Create virtual environment
python -m venv drug_analysis_env

# Activate virtual environment
# Windows:
drug_analysis_env\Scripts\activate
# macOS/Linux:
source drug_analysis_env/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 5. Google Gemini API Setup

1. **Get API Key**:
   - Visit [Google AI Studio](https://makersuite.google.com/app/apikey)
   - Create a new API key
   - Copy the key to your `.env` file

2. **Test API Connection**:
```bash
python -c "
import os
from dotenv import load_dotenv
load_dotenv()
print('API Key loaded:', 'GOOGLE_API_KEY' in os.environ)
"
```

### 6. Verification Steps

#### Test Database Connection
```bash
python -c "
import sys
sys.path.append('.')
from src.core.config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME
import psycopg2
try:
    conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
    print('✅ Database connection successful')
    conn.close()
except Exception as e:
    print('❌ Database connection failed:', e)
"
```

#### Run System Analysis
```bash
python scripts/production/db_analyzer.py
```

### 7. Common Issues & Solutions

#### Issue: psycopg2 Installation Error
```bash
# Solution: Install binary version
pip install psycopg2-binary
```

#### Issue: Permission Denied for Logs Directory
```bash
# Solution: Create logs directory with proper permissions
mkdir -p logs
chmod 755 logs
```

#### Issue: Gemini API Rate Limiting
- **Symptom**: 429 Too Many Requests errors
- **Solution**: Scripts automatically handle rate limiting with 4-second delays
- **Check**: Verify your API quota in Google Cloud Console

#### Issue: Database Connection Timeout
```bash
# Check PostgreSQL service status
# Windows:
net start postgresql-x64-13

# macOS:
brew services start postgresql

# Linux:
sudo systemctl start postgresql
```

### 8. Performance Tuning

#### For Large Datasets (>10,000 ingredients)
```bash
# Adjust batch sizes
python scripts/production/claude_interactive_mapper.py --sample 100 --batch-size 10

# Monitor memory usage
python -c "import psutil; print(f'Memory: {psutil.virtual_memory().percent}%')"
```

#### PostgreSQL Optimization
```sql
-- Connect to your database
\c pharmacy_db

-- Check table sizes
SELECT schemaname,tablename,attname,n_distinct,correlation 
FROM pg_stats 
WHERE schemaname = 'public' 
ORDER BY n_distinct DESC;

-- Create indexes for better performance (if not exist)
CREATE INDEX IF NOT EXISTS idx_ingredient_mappings_confidence ON ingredient_mappings(confidence DESC);
CREATE INDEX IF NOT EXISTS idx_active_ingredients_name ON active_ingredients USING gin(to_tsvector('english', name));
```

### 9. Development Setup

#### Pre-commit Hooks (Optional)
```bash
pip install pre-commit black flake8
pre-commit install
```

#### Testing Setup
```bash
# Install test dependencies
pip install pytest pytest-cov

# Run tests
pytest tests/
```

### 10. Next Steps

Once setup is complete:

1. **Start with Small Sample**:
```bash
python scripts/production/claude_interactive_mapper.py --sample 10
```

2. **Review Logs**:
```bash
tail -f logs/claudeinteractivemapper_*.log
```

3. **Check Results**:
```bash
# Connect to database and check mappings
psql -U postgres -d pharmacy_db -c "SELECT COUNT(*) FROM ingredient_mappings;"
```

4. **Scale Up**:
```bash
# Process larger batches
python scripts/production/claude_interactive_mapper.py --sample 100
```

## Support

If you encounter issues:
1. Check the logs in `logs/` directory
2. Verify all environment variables are set correctly
3. Test database and API connections independently
4. Review CLAUDE.md for advanced troubleshooting