# generate_test_data.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

random.seed(42)
np.random.seed(42)

# Generate customers
n_customers = 10000
customers = pd.DataFrame({
    "customer_id": range(1, n_customers + 1),
    "first_name": np.random.choice(["John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Ethan", "Fiona"], n_customers),
    "last_name": np.random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"], n_customers),
    "email": [f"customer{i}@email.com" for i in range(1, n_customers + 1)],
    "phone": [f"555-{random.randint(1000, 9999)}" for _ in range(n_customers)],
    "date_of_birth": pd.date_range("1960-01-01", "2000-12-31", periods=n_customers).date,
    "address": [f"Street {i}" for i in range(1, n_customers + 1)],
    "city": np.random.choice(["Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata", "Hyderabad", "Pune", "Ahmedabad"], n_customers),
    "state": np.random.choice(["MH", "DL", "KA", "TN", "WB", "TG", "MH", "GJ"], n_customers),
    "country": "India",
    "postal_code": [f"{random.randint(100000, 999999)}" for _ in range(n_customers)],
    "account_type": np.random.choice(["savings", "checking", "both"], n_customers, p=[0.5, 0.3, 0.2]),
    "kyc_status": np.random.choice(["verified", "pending", "rejected"], n_customers, p=[0.85, 0.1, 0.05]),
    "created_at": [datetime.now() - timedelta(days=random.randint(0, 365)) for _ in range(n_customers)],
    "updated_at": [datetime.now() - timedelta(days=random.randint(0, 30)) for _ in range(n_customers)]
})

# Generate loans
n_loans = 15000
loan_types = ["personal", "home", "auto", "education", "business"]
customers_with_loans = np.random.choice(customers["customer_id"], n_loans)

loans = pd.DataFrame({
    "loan_id": range(1, n_loans + 1),
    "customer_id": customers_with_loans,
    "loan_type": np.random.choice(loan_types, n_loans, p=[0.3, 0.25, 0.2, 0.15, 0.1]),
    "loan_amount": np.random.exponential(500000, n_loans).clip(50000, 5000000),
    "interest_rate": np.random.normal(12, 3, n_loans).clip(8, 24),
    "tenure_months": np.random.choice([12, 24, 36, 60, 120, 180, 240], n_loans, p=[0.1, 0.15, 0.2, 0.2, 0.15, 0.1, 0.1]),
    "emi_amount": 0,  # calculated later
    "sanctioned_date": pd.date_range("2020-01-01", "2026-05-01", periods=n_loans).date,
    "maturity_date": pd.date_range("2021-01-01", "2046-05-01", periods=n_loans).date,
    "loan_status": np.random.choice(["active", "closed", "defaulted", "pending"], n_loans, p=[0.6, 0.25, 0.05, 0.1]),
    "credit_score": np.random.choice(range(300, 900), n_loans),
    "collateral_type": np.random.choice(["none", "property", "vehicle", "gold", "fd"], n_loans),
    "processing_fee": 0,
    "gst_amount": 0,
    "total_payable": 0,
    "created_at": [datetime.now() - timedelta(days=random.randint(0, 365)) for _ in range(n_loans)]
})

# Calculate derived fields
loans["emi_amount"] = (
    loans["loan_amount"] * (loans["interest_rate"]/1200) * 
    (1 + loans["interest_rate"]/1200)**loans["tenure_months"] / 
    ((1 + loans["interest_rate"]/1200)**loans["tenure_months"] - 1)
).round(2)
loans["processing_fee"] = (loans["loan_amount"] * 0.01).round(2)
loans["gst_amount"] = (loans["processing_fee"] * 0.18).round(2)
loans["total_payable"] = (loans["emi_amount"] * loans["tenure_months"]).round(2)

# Generate deposits
n_deposits = 20000
deposit_types = ["fd", "rd", "savings", "current"]
customers_with_deposits = np.random.choice(customers["customer_id"], n_deposits)

deposits = pd.DataFrame({
    "deposit_id": range(1, n_deposits + 1),
    "customer_id": customers_with_deposits,
    "deposit_type": np.random.choice(deposit_types, n_deposits, p=[0.35, 0.25, 0.3, 0.1]),
    "principal_amount": np.random.exponential(300000, n_deposits).clip(10000, 2000000),
    "interest_rate": np.random.normal(7, 1.5, n_deposits).clip(3, 12),
    "tenure_months": np.random.choice([6, 12, 24, 36, 60], n_deposits),
    "maturity_amount": 0,
    "interest_earned": 0,
    "start_date": pd.date_range("2020-01-01", "2026-05-01", periods=n_deposits).date,
    "maturity_date": pd.date_range("2020-07-01", "2031-05-01", periods=n_deposits).date,
    "auto_renewal": np.random.choice([True, False], n_deposits, p=[0.4, 0.6]),
    "premature_withdrawal": np.random.choice([True, False], n_deposits, p=[0.1, 0.9]),
    "penalty_amount": 0,
    "nominee_name": np.random.choice(["Spouse", "Parent", "Child", "Sibling"], n_deposits),
    "relationship": np.random.choice(["wife", "husband", "father", "mother", "son", "daughter"], n_deposits),
    "created_at": [datetime.now() - timedelta(days=random.randint(0, 365)) for _ in range(n_deposits)]
})

# Calculate derived fields
deposits["interest_earned"] = (
    deposits["principal_amount"] * deposits["interest_rate"] * deposits["tenure_months"] / 1200
).round(2)
deposits["maturity_amount"] = (deposits["principal_amount"] + deposits["interest_earned"]).round(2)
deposits["penalty_amount"] = np.where(
    deposits["premature_withdrawal"], 
    (deposits["interest_earned"] * 0.5).round(2), 
    0
)

# Save to CSV
customers.to_csv("customers.csv", index=False)
loans.to_csv("loans.csv", index=False)
deposits.to_csv("deposits.csv", index=False)

print("Generated:")
print(f"  Customers: {len(customers)}")
print(f"  Loans: {len(loans)}")
print(f"  Deposits: {len(deposits)}")