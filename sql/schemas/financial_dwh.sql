-- Financial Data Warehouse Schema

-- Dimension Tables
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id VARCHAR(20) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    date_joined DATE,
    account_type VARCHAR(50),
    risk_category VARCHAR(20),
    credit_score INTEGER,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    quarter INTEGER,
    day_of_week INTEGER,
    month_name VARCHAR(20),
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- Fact Tables
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_key INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id VARCHAR(20) UNIQUE NOT NULL,
    customer_id VARCHAR(20),
    transaction_date TIMESTAMP,
    amount DECIMAL(15,2),
    transaction_type VARCHAR(50),
    channel VARCHAR(50),
    merchant VARCHAR(255),
    category VARCHAR(100),
    status VARCHAR(20),
    is_fraud INTEGER,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id)
);



-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_fact_transactions_customer ON fact_transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_date ON fact_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_amount ON fact_transactions(amount);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_fraud ON fact_transactions(is_fraud);