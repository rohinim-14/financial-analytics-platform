import pytest
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.analytics.data_quality import DataQualityAnalyzer
from src.analytics.financial_analytics import FinancialAnalytics


def test_data_quality_analyzer():
    """Test data quality analyzer initialization"""
    analyzer = DataQualityAnalyzer()
    assert analyzer.quality_rules['completeness_threshold'] == 95


def test_financial_analytics():
    """Test financial analytics initialization"""
    analytics = FinancialAnalytics()
    assert analytics.db_path == 'data/financial_analytics.db'


if __name__ == "__main__":
    pytest.main([__file__])