# financial-analytics-platform
comprehensive financial data analytics platform for regulatory reporting and business intelligence
# Financial Data Analytics Platform

A comprehensive platform for financial data analysis, regulatory reporting, and business intelligence.

## Current Status: Phase 1 Complete 

### Implemented Features:
- Sample data generation (customers & transactions)
- Database schema design
- Data quality analysis framework

### Quick Start:
```bash
# Setup environment
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt

# Generate sample data
python src/utils/data_generator.py

# Setup database
python src/utils/database_setup.py

# Run analytics
python src/analytics/financial_analytics.py

# Run tests
pytest tests/
```