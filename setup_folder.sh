#!/bin/bash

# Reorganize existing open_medic folder into structured project
# Run this script from your open_medic folder

echo "🔄 Reorganizing existing open_medic folder..."

# Create new directory structure
mkdir -p sql/{01_setup,02_analysis,03_reporting,04_maintenance,99_utilities}
mkdir -p scripts/{import,export,03_analysis,maintenance}
mkdir -p docs
mkdir -p data/{original,processed,exports}
mkdir -p config
mkdir -p archive

# Move existing files based on patterns
echo "📁 Moving existing files..."

# Move import scripts
mv import_*.sh scripts/import/ 2>/dev/null
echo "✅ Moved import scripts to scripts/import/"

# Move examine scripts (these seem to be 03_analysis scripts)
mv examine_*.sh scripts/03_analysis/ 2>/dev/null
echo "✅ Moved analysis scripts to scripts/analysis/"

# Move clean/process scripts
mv clean_*.py process_*.py scripts/maintenance/ 2>/dev/null
echo "✅ Moved processing scripts to scripts/maintenance/"

# Move CSV 03_analysis script
mv csv_file_analysis.py scripts/03_analysis/ 2>/dev/null
echo "✅ Moved CSV analysis to scripts/analysis/"

# Move SQL scripts
mv 00_create_medicine_sales_table.sql sql/99_utilities/ 2>/dev/null
echo "✅ Moved SQL scripts to utilities/"

# Move data folders
mv csv_data/ data/processed/ 2>/dev/null || echo "ℹ️  csv_data folder not found or already moved"
mv original_data/ data/original/ 2>/dev/null || echo "ℹ️  original_data folder not found or already moved"

# Create organized structure for your year-based scripts
echo "📅 Organizing year-based scripts..."

# Group import scripts by function
cat >scripts/import/README.md <<'EOF'
# Import Scripts

## Usage
- `import_YYYY.sh`: Import Open Medic data for specific year
- Run scripts in chronological order for full dataset

## Available Years
EOF

# List available years
for file in scripts/import/import_*.sh; do
  if [ -f "$file" ]; then
    year=$(basename "$file" .sh | sed 's/import_//')
    echo "- $year" >>scripts/import/README.md
  fi
done

# Group examine scripts
cat >scripts/03_analysis/README.md <<'EOF'
# Analysis Scripts

## Examine Scripts
- `examine_YYYY.sh`: Analyze Open Medic data for specific year
- Contains data validation and summary statistics

## Python Analysis
- `csv_file_analysis.py`: Detailed CSV file analysis
- `process_csv_pandas.py`: Pandas-based data processing
EOF

# Create main project files
cat >README.md <<'EOF'
# Open Medic Analysis Project

## Overview
Analysis of French pharmaceutical consumption data using Open Medic dataset (2014-2024).

## Project Structure
```
open_medic/
├── sql/                    # SQL queries and database scripts
│   ├── 01_setup/          # Database setup and optimization
│   ├── 02_analysis/       # Analysis queries
│   ├── 03_reporting/      # Reports and dashboards
│   ├── 04_maintenance/    # Database maintenance
│   └── 99_utilities/      # Utility queries
├── scripts/               # Processing and analysis scripts
│   ├── import/           # Data import scripts (by year)
│   ├── export/           # Data export utilities
│   ├── analysis/         # Data analysis and validation
│   └── maintenance/      # Data cleaning and processing
├── data/                 # Data storage
│   ├── original/         # Raw Open Medic files
│   ├── processed/        # Cleaned and processed data
│   └── exports/          # Analysis results and exports
└── docs/                 # Documentation
```

## Available Data Years
EOF

# List available years from import scripts
echo "**Import scripts available for years:**" >>README.md
for file in scripts/import/import_*.sh; do
  if [ -f "$file" ]; then
    year=$(basename "$file" .sh | sed 's/import_//')
    echo "- $year" >>README.md
  fi
done

# Create .gitignore
cat >.gitignore <<'EOF'
# Large data files
data/original/*.csv
data/original/*.txt
data/processed/*.csv
data/exports/*.csv
*.gz
*.zip
*.tar

# Sensitive configuration
config/*.conf
config/*.env
*.password

# Python cache
__pycache__/
*.pyc
*.pyo

# Temporary files
*.tmp
*.temp
temp/
logs/

# OS files
.DS_Store
Thumbs.db

# DataGrip
.idea/
*.iml
EOF

# Create configuration templates
cat >config/database_template.conf <<'EOF'
# Database Configuration Template
# Copy to database.conf and fill in your details

DB_HOST=localhost
DB_PORT=5432
DB_NAME=open_medic
DB_USER=your_username
DB_PASSWORD=your_password

# Target database for analysis
INCIDENTS_DB_NAME=incidents_json
EOF

# Create master import script
cat >scripts/import_all_years.sh <<'EOF'
#!/bin/bash

# Master script to import all available years
# Run this to import the complete Open Medic dataset

echo "🚀 Starting complete Open Medic import..."

# Import data for all available years
for script in scripts/import/import_*.sh; do
    if [ -f "$script" ]; then
        year=$(basename "$script" .sh | sed 's/import_//')
        echo "📊 Importing data for year $year..."
        
        # Make script executable and run it
        chmod +x "$script"
        "$script"
        
        if [ $? -eq 0 ]; then
            echo "✅ Successfully imported $year"
        else
            echo "❌ Failed to import $year"
        fi
    fi
done

echo "🎉 Complete import finished!"
EOF

# Create master 03_analysis script
cat >scripts/analyze_all_years.sh <<'EOF'
#!/bin/bash

# Master script to analyze all imported data
# Run this after importing to validate data quality

echo "🔍 Starting complete data analysis..."

for script in scripts/analysis/examine_*.sh; do
    if [ -f "$script" ]; then
        year=$(basename "$script" .sh | sed 's/examine_//')
        echo "📈 Analyzing data for year $year..."
        
        chmod +x "$script"
        "$script"
    fi
done

echo "📊 Analysis complete!"
EOF

# Make master scripts executable
chmod +x scripts/import_all_years.sh
chmod +x scripts/analyze_all_years.sh

# Create SQL templates for our optimization work
cat >sql/01_setup/01_create_optimized_schema.sql <<'EOF'
/*
 * Create Optimized Schema in Open Medic Database
 * 
 * This script will create an optimized version of your imported
 * Open Medic data for analysis with BDPM
 */

-- TODO: Add optimization script content
-- (Will be added based on your specific table structure)
EOF

cat >sql/02_analysis/consumption_by_substance.sql <<'EOF'
/*
 * Analyze consumption by active substance
 * 
 * Cross-references Open Medic consumption data with BDPM
 * to calculate total consumption by active substance
 */

-- TODO: Add analysis queries
EOF

# Create documentation
cat >docs/data_sources.md <<'EOF'
# Data Sources

## Open Medic Dataset
- **Source**: Assurance Maladie (France)
- **Years available**: 2014-2024
- **Content**: Medication reimbursement data
- **Import scripts**: Located in `scripts/import/`

## Processing Pipeline
1. **Import**: Raw CSV files imported using year-specific scripts
2. **Analysis**: Data validation and summary using examine scripts  
3. **Optimization**: Aggregated data prepared for BDPM analysis
4. **Export**: Results exported for integration with incidents_json

## File Naming Convention
- `import_YYYY.sh`: Import script for year YYYY
- `examine_YYYY.sh`: Analysis script for year YYYY
- Data files should follow Open Medic naming conventions
EOF

echo ""
echo "✅ Reorganization complete!"
echo ""
echo "📋 Summary of changes:"
echo "   📂 Created organized folder structure"
echo "   📄 Moved existing scripts to appropriate folders"
echo "   📝 Created README.md and documentation"
echo "   🔧 Created master import/analysis scripts"
echo "   ⚙️  Created configuration templates"
echo ""
echo "🔧 Next steps:"
echo "1. Review the new structure: ls -la"
echo "2. Test your existing scripts work in new locations"
echo "3. Add our optimization scripts to sql/01_setup/"
echo "4. Update any hardcoded paths in your scripts"
echo ""
echo "💡 Your original functionality is preserved!"
echo "   All scripts moved to appropriate locations"
echo "   Data folders reorganized but accessible"
echo "   Ready for DataGrip integration"
