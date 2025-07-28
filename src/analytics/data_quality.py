import pandas as pd
import numpy as np
from typing import Dict, List, Any
import sqlite3


class DataQualityAnalyzer:
    def __init__(self, db_path='data/financial_analytics.db'):
        self.db_path = db_path
        self.quality_rules = {
            'completeness_threshold': 95,  # 95% completeness required
            'accuracy_threshold': 98,  # 98% accuracy required
            'consistency_threshold': 99  # 99% consistency required
        }

    def connect_to_db(self):
        return sqlite3.connect(self.db_path)

    def check_completeness(self, table_name: str, critical_columns: List[str]) -> Dict:
        """Check data completeness for critical columns"""
        conn = self.connect_to_db()

        results = {}
        for column in critical_columns:
            query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT({column}) as non_null_records,
                (COUNT({column}) * 100.0 / COUNT(*)) as completeness_pct
            FROM {table_name}
            """
            result = pd.read_sql_query(query, conn)
            results[column] = {
                'completeness_pct': result['completeness_pct'].iloc[0],
                'status': 'PASS' if result['completeness_pct'].iloc[0] >= self.quality_rules[
                    'completeness_threshold'] else 'FAIL'
            }

        conn.close()
        return results

    def check_duplicates(self, table_name: str, key_columns: List[str]) -> Dict:
        """Check for duplicate records"""
        conn = self.connect_to_db()

        columns_str = ', '.join(key_columns)
        query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT {columns_str}) as unique_records,
            (COUNT(*) - COUNT(DISTINCT {columns_str})) as duplicate_count
        FROM {table_name}
        """

        result = pd.read_sql_query(query, conn)
        conn.close()

        return {
            'total_records': result['total_records'].iloc[0],
            'unique_records': result['unique_records'].iloc[0],
            'duplicate_count': result['duplicate_count'].iloc[0],
            'status': 'PASS' if result['duplicate_count'].iloc[0] == 0 else 'FAIL'
        }

    def generate_quality_report(self) -> Dict:
        """Generate comprehensive data quality report"""
        report = {
            'timestamp': pd.Timestamp.now(),
            'customer_data_quality': {},
            'transaction_data_quality': {},
            'overall_score': 0
        }

        # Check customer data quality
        customer_completeness = self.check_completeness(
            'dim_customer',
            ['customer_id', 'first_name', 'last_name', 'email']
        )
        customer_duplicates = self.check_duplicates('dim_customer', ['customer_id'])

        report['customer_data_quality'] = {
            'completeness': customer_completeness,
            'duplicates': customer_duplicates
        }

        # Check transaction data quality
        transaction_completeness = self.check_completeness(
            'fact_transactions',
            ['transaction_id', 'customer_id', 'amount', 'transaction_date']
        )
        transaction_duplicates = self.check_duplicates('fact_transactions', ['transaction_id'])

        report['transaction_data_quality'] = {
            'completeness': transaction_completeness,
            'duplicates': transaction_duplicates
        }

        return report


if __name__ == "__main__":
    analyzer = DataQualityAnalyzer()
    report = analyzer.generate_quality_report()
    print("Data Quality Report Generated:")
    for key, value in report.items():
        print(f"{key}: {value}")