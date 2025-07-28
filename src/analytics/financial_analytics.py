import pandas as pd
import numpy as np
import sqlite3
from typing import Dict, List
import plotly.express as px
import plotly.graph_objects as go


class FinancialAnalytics:
    def __init__(self, db_path='data/financial_analytics.db'):
        self.db_path = db_path

    def connect_to_db(self):
        return sqlite3.connect(self.db_path)

    def calculate_kpis(self) -> Dict:
        """Calculate key financial KPIs"""
        conn = self.connect_to_db()

        # Total transaction volume
        volume_query = """
        SELECT 
            COUNT(*) as total_transactions,
            SUM(amount) as total_volume,
            AVG(amount) as avg_transaction_amount
        FROM fact_transactions 
        WHERE status = 'Completed'
        """
        volume_data = pd.read_sql_query(volume_query, conn)

        # Monthly trends
        monthly_query = """
        SELECT 
            strftime('%Y-%m', transaction_date) as month,
            COUNT(*) as transaction_count,
            SUM(amount) as monthly_volume
        FROM fact_transactions 
        WHERE status = 'Completed'
        GROUP BY strftime('%Y-%m', transaction_date)
        ORDER BY month
        """
        monthly_data = pd.read_sql_query(monthly_query, conn)

        # Fraud statistics
        fraud_query = """
        SELECT 
            is_fraud,
            COUNT(*) as count,
            SUM(amount) as total_amount
        FROM fact_transactions
        GROUP BY is_fraud
        """
        fraud_data = pd.read_sql_query(fraud_query, conn)

        conn.close()

        return {
            'volume_metrics': volume_data.to_dict('records')[0],
            'monthly_trends': monthly_data.to_dict('records'),
            'fraud_stats': fraud_data.to_dict('records')
        }

    def customer_segmentation_analysis(self) -> pd.DataFrame:
        """Perform customer segmentation analysis"""
        conn = self.connect_to_db()

        query = """
        SELECT 
            c.risk_category,
            c.account_type,
            COUNT(DISTINCT c.customer_id) as customer_count,
            AVG(c.credit_score) as avg_credit_score,
            COUNT(t.transaction_id) as total_transactions,
            AVG(t.amount) as avg_transaction_amount,
            SUM(CASE WHEN t.is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count
        FROM dim_customer c
        LEFT JOIN fact_transactions t ON c.customer_id = t.customer_id
        GROUP BY c.risk_category, c.account_type
        ORDER BY customer_count DESC
        """

        result = pd.read_sql_query(query, conn)
        conn.close()

        return result

    def transaction_pattern_analysis(self) -> Dict:
        """Analyze transaction patterns"""
        conn = self.connect_to_db()

        # Channel analysis
        channel_query = """
        SELECT 
            channel,
            COUNT(*) as transaction_count,
            AVG(amount) as avg_amount,
            SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count
        FROM fact_transactions
        GROUP BY channel
        ORDER BY transaction_count DESC
        """

        # Category analysis
        category_query = """
        SELECT 
            category,
            COUNT(*) as transaction_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM fact_transactions
        WHERE amount > 0
        GROUP BY category
        ORDER BY total_amount DESC
        """

        channel_data = pd.read_sql_query(channel_query, conn)
        category_data = pd.read_sql_query(category_query, conn)

        conn.close()

        return {
            'channel_analysis': channel_data.to_dict('records'),
            'category_analysis': category_data.to_dict('records')
        }


if __name__ == "__main__":
    analytics = FinancialAnalytics()
    kpis = analytics.calculate_kpis()
    print("KPI Calculation completed")
    print(f"Total Transactions: {kpis['volume_metrics']['total_transactions']}")




