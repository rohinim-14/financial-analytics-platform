import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

fake = Faker()
Faker.seed(42)
np.random.seed(42)


class FinancialDataGenerator:
    def __init__(self):
        self.fake = fake
        self.start_date = datetime(2023, 1, 1)
        self.end_date = datetime(2024, 12, 31)

    def generate_customers(self, n=10000):
        """Generate customer data"""
        customers = []
        for i in range(n):
            customer = {
                'customer_id': f'CUST_{i + 1:06d}',
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'email': self.fake.email(),
                'phone': self.fake.phone_number(),
                'address': self.fake.address().replace('\n', ', '),
                'city': self.fake.city(),
                'state': self.fake.state(),
                'country': self.fake.country(),
                'date_joined': self.fake.date_between(self.start_date, self.end_date),
                'account_type': random.choice(['Savings', 'Checking', 'Credit', 'Investment']),
                'risk_category': random.choice(['Low', 'Medium', 'High']),
                'credit_score': random.randint(300, 850)
            }
            customers.append(customer)
        return pd.DataFrame(customers)

    def generate_transactions(self, n=100000):
        """Generate transaction data"""
        transactions = []
        customer_ids = [f'CUST_{i + 1:06d}' for i in range(10000)]

        for i in range(n):
            transaction = {
                'transaction_id': f'TXN_{i + 1:08d}',
                'customer_id': random.choice(customer_ids),
                'transaction_date': self.fake.date_time_between(self.start_date, self.end_date),
                'amount': round(random.uniform(-5000, 10000), 2),
                'transaction_type': random.choice(['Deposit', 'Withdrawal', 'Transfer', 'Payment', 'Fee']),
                'channel': random.choice(['Online', 'ATM', 'Branch', 'Mobile', 'Phone']),
                'merchant': self.fake.company(),
                'category': random.choice(['Groceries', 'Gas', 'Shopping', 'Dining', 'Travel', 'Bills']),
                'status': random.choice(['Completed', 'Pending', 'Failed']),
                'is_fraud': random.choice([0, 0, 0, 0, 0, 0, 0, 0, 0, 1])  # 10% fraud rate
            }
            transactions.append(transaction)
        return pd.DataFrame(transactions)

    def save_datasets(self):
        """Generate and save all datasets"""
        # Create data directories if they don't exist
        os.makedirs('data/raw', exist_ok=True)

        print("Generating customer data...")
        customers = self.generate_customers()
        customers.to_csv('data/raw/customers.csv', index=False)
        print(f"Created customers.csv with {len(customers)} records")

        print("Generating transaction data...")
        transactions = self.generate_transactions()
        transactions.to_csv('data/raw/transactions.csv', index=False)
        print(f"Created transactions.csv with {len(transactions)} records")

        print("Data generation completed!")


if __name__ == "__main__":
    generator = FinancialDataGenerator()
    generator.save_datasets()