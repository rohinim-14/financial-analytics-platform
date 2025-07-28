import sqlite3
import pandas as pd
from pathlib import Path
import os


class DatabaseSetup:
    def __init__(self, db_path='data/financial_analytics.db'):
        # Get the project root directory (2 levels up from current file: src/utils -> src -> root)
        project_root = Path(__file__).parent.parent.parent
        self.db_path = project_root / db_path
        self.project_root = project_root
        self.connection = None
        # Create data directory if it doesn't exist
        os.makedirs(self.db_path.parent, exist_ok=True)

    def connect(self):
        """Create database connection"""
        self.connection = sqlite3.connect(self.db_path)
        print(f"Connected to database: {self.db_path}")
        return self.connection

    def create_tables(self):
        """Create database tables"""
        # Find schema file in project root
        schema_path = self.project_root / 'sql/schemas/financial_dwh.sql'

        if schema_path.exists():
            with open(schema_path, 'r') as f:
                schema_sql = f.read()

            # Execute schema (split by semicolon for SQLite)
            statements = schema_sql.split(';')
            for statement in statements:
                statement = statement.strip()
                if statement and not statement.startswith('--'):
                    try:
                        self.connection.execute(statement)
                        print(f"Executed: {statement[:50]}...")
                    except Exception as e:
                        print(f"Error executing: {statement[:50]}... Error: {e}")

            self.connection.commit()
            print("✅ Database tables created successfully!")
        else:
            print(f"❌ Schema file not found: {schema_path}")

    def load_sample_data(self):
        """Load sample data into database"""
        try:
            # Use project root for file paths
            customers_file = self.project_root / 'data/raw/customers.csv'
            transactions_file = self.project_root / 'data/raw/transactions.csv'

            if not customers_file.exists():
                print(f"❌ Customer data file not found: {customers_file}")
                print("💡 Run 'python src/utils/data_generator.py' first!")
                return

            if not transactions_file.exists():
                print(f"❌ Transaction data file not found: {transactions_file}")
                print("💡 Run 'python src/utils/data_generator.py' first!")
                return

            # Load customers in chunks
            print("Loading customer data...")
            customers_df = pd.read_csv(customers_file)

            # Map column names to match database schema
            customers_df.columns = customers_df.columns.str.lower()

            # Load in smaller chunks to avoid SQLite variable limit
            customers_df.to_sql('dim_customer', self.connection, if_exists='replace', index=False, chunksize=1000)
            print(f"✅ Loaded {len(customers_df)} customer records")

            # Load transactions in chunks
            print("Loading transaction data...")
            transactions_df = pd.read_csv(transactions_file)

            # Map column names to match database schema
            transactions_df.columns = transactions_df.columns.str.lower()

            # Load in smaller chunks to avoid SQLite variable limit
            transactions_df.to_sql('fact_transactions', self.connection, if_exists='replace', index=False,
                                   chunksize=1000)
            print(f"✅ Loaded {len(transactions_df)} transaction records")

            print("✅ Sample data loaded successfully!")

        except Exception as e:
            print(f"❌ Error loading sample data: {e}")

    def verify_data(self):
        """Verify that data was loaded correctly"""
        try:
            # Check customer count
            customer_count = self.connection.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
            print(f"📊 Customer records: {customer_count}")

            # Check transaction count
            transaction_count = self.connection.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0]
            print(f"📊 Transaction records: {transaction_count}")

            # Show sample customer data
            print("\n📝 Sample customer data:")
            sample_customers = self.connection.execute("""
                SELECT customer_id, first_name, last_name, account_type, risk_category 
                FROM dim_customer 
                LIMIT 5
            """).fetchall()

            for customer in sample_customers:
                print(f"  {customer}")

            # Show sample transaction data
            print("\n📝 Sample transaction data:")
            sample_transactions = self.connection.execute("""
                SELECT transaction_id, customer_id, amount, transaction_type, status 
                FROM fact_transactions 
                LIMIT 5
            """).fetchall()

            for transaction in sample_transactions:
                print(f"  {transaction}")

        except Exception as e:
            print(f"❌ Error verifying data: {e}")

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("🔐 Database connection closed")


if __name__ == "__main__":
    print("🚀 Setting up Financial Analytics Database...")
    print("=" * 50)

    db_setup = DatabaseSetup()

    try:
        # Connect to database
        db_setup.connect()

        # Create tables
        print("\n📋 Creating database tables...")
        db_setup.create_tables()

        # Load sample data
        print("\n📥 Loading sample data...")
        db_setup.load_sample_data()

        # Verify data
        print("\n🔍 Verifying data...")
        db_setup.verify_data()

        print("\n🎉 Database setup completed successfully!")

    except Exception as e:
        print(f"❌ Database setup failed: {e}")

    finally:
        db_setup.close()