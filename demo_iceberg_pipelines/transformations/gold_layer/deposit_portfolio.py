from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession, functions as F

spark = SparkSession.active()

@dp.materialized_view(name="deposit_portfolio_summary")
def build_deposit_summary() -> DataFrame:
    """
    Gold: Deposit book analytics and maturity ladder.
    """
    df = spark.table("deposits_silver")

    return df.groupBy("deposit_type", "maturity_status").agg(
        F.count("deposit_id").alias("deposit_count"),
        F.sum("principal_amount").alias("total_principal"),
        F.sum("interest_earned").alias("total_interest"),
        F.sum("maturity_amount").alias("total_maturity_value"),
        F.avg("interest_rate").alias("avg_interest_rate"),
        F.avg("days_to_maturity").alias("avg_days_remaining"),
        F.sum(
            F.when(F.col("auto_renewal") == True, F.col("principal_amount")).otherwise(0)
        ).alias("auto_renewal_value"),
        F.sum(
            F.when(F.col("premature_withdrawal") == True, 1).otherwise(0)
        ).alias("premature_withdrawal_count")
    ).withColumn("interest_cost_pct",
        F.when(F.col("total_principal") > 0, F.col("total_interest") / F.col("total_principal") * 100).otherwise(0)
    )
