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
