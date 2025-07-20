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

## Available Data Years
**Import scripts available for years:**
- 2014
- 2015
- 2016
- 2017
- 2018
- 2019
- 2020
- 2021
- 2022
- 2023
- 2024
- data
