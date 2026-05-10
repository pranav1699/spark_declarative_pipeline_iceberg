from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql import Window  

spark = SparkSession.active()

@dp.materialized_view(name="loan_portfolio_summary")
def build_loan_portfolio() -> DataFrame:
    df = spark.table("loans_silver")
    loan_portfolio_summary = df.groupBy("loan_type", "loan_status", "risk_category", "loan_category").agg(
        F.count("loan_id").alias("loan_count"),
        F.sum("loan_amount").alias("total_sanctioned"),
        F.avg("loan_amount").alias("avg_loan_size"),
        F.sum(
            F.when(F.col("loan_status") == "active", F.col("loan_amount")).otherwise(0)
        ).alias("active_portfolio_value"),
        F.avg("interest_rate").alias("avg_interest_rate"),
        F.min("interest_rate").alias("min_rate"),
        F.max("interest_rate").alias("max_rate"),
        F.avg("credit_score").alias("avg_credit_score"),
        F.avg("days_since_sanction").alias("avg_loan_age_days"),
        F.sum(
            F.when(F.col("loan_status") == "defaulted", 1).otherwise(0)
        ).alias("default_count"),
        F.sum(
            F.when(F.col("loan_status") == "defaulted", F.col("loan_amount")).otherwise(0)
        ).alias("default_amount")
    ).withColumn("default_rate_pct",
        F.when(F.col("loan_count") > 0, F.col("default_count") / F.col("loan_count") * 100).otherwise(0)
    ).withColumn("portfolio_share_pct",
        F.col("total_sanctioned") / F.sum("total_sanctioned").over(Window.partitionBy()) * 100  
    )
    return loan_portfolio_summary

@dp.materialized_view(name="loan_monthly_trends")
def build_loan_trends() -> DataFrame:
    """
    Gold: Month-over-month loan sanctions for trend analysis.
    """
    df = spark.table("loans_silver")
    loan_monthly_trends = df \
        .withColumn("sanction_year_month", F.date_format(F.col("sanctioned_date"), "yyyy-MM")) \
        .groupBy("sanction_year_month", "loan_type") \
        .agg(
            F.count("loan_id").alias("loans_sanctioned"),
            F.sum("loan_amount").alias("amount_sanctioned"),
            F.avg("interest_rate").alias("avg_rate"),
            F.count(F.when(F.col("risk_category") == "high_risk", 1)).alias("high_risk_count")
        ) \
        .withColumn("prev_month_amount",
            F.lag("amount_sanctioned", 1).over(
                Window.partitionBy("loan_type").orderBy("sanction_year_month") 
            )
        ) \
        .withColumn("mom_growth_pct",
            F.when(
                F.col("prev_month_amount").isNotNull() & (F.col("prev_month_amount") > 0),
                (F.col("amount_sanctioned") - F.col("prev_month_amount")) / F.col("prev_month_amount") * 100
            ).otherwise(0)
        )
    return loan_monthly_trends
