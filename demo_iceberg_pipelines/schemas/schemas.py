from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DateType, TimestampType, DoubleType, BooleanType



CUSTOMERS_BRONZE_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("account_type", StringType(), True),  # savings, checking, both
    StructField("kyc_status", StringType(), True),      # verified, pending, rejected
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("source_file", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True)
])


LOANS_BRONZE_SCHEMA = StructType([
    StructField("loan_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("loan_type", StringType(), True),       # personal, home, auto, education, business
    StructField("loan_amount", DoubleType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("tenure_months", IntegerType(), True),
    StructField("emi_amount", DoubleType(), True),
    StructField("sanctioned_date", DateType(), True),
    StructField("maturity_date", DateType(), True),
    StructField("loan_status", StringType(), True),     # active, closed, defaulted, pending
    StructField("credit_score", IntegerType(), True),
    StructField("collateral_type", StringType(), True),
    StructField("processing_fee", DoubleType(), True),
    StructField("gst_amount", DoubleType(), True),
    StructField("total_payable", DoubleType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("source_file", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True)
])


DEPOSITS_BRONZE_SCHEMA = StructType([
    StructField("deposit_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("deposit_type", StringType(), True),     
    StructField("principal_amount", DoubleType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("tenure_months", IntegerType(), True),
    StructField("maturity_amount", DoubleType(), True),
    StructField("interest_earned", DoubleType(), True),
    StructField("start_date", DateType(), True),
    StructField("maturity_date", DateType(), True),
    StructField("auto_renewal", BooleanType(), True),
    StructField("premature_withdrawal", BooleanType(), True),
    StructField("penalty_amount", DoubleType(), True),
    StructField("nominee_name", StringType(), True),
    StructField("relationship", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("source_file", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True)
])
