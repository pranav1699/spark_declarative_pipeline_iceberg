from pyspark import pipelines as dp
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


spark = (
    SparkSession.active()
)




@dp.temporary_view
def loans_silver() -> DataFrame:
    """Add derived columns to loan data"""
    df = spark.table("loans_bronze")
    return (df 
        .withColumn("loan_category", 
            F.when(F.col("loan_amount") <= 100000, "small")
             .when(F.col("loan_amount") <= 500000, "medium")
             .otherwise("large")
        ) 
        .withColumn("risk_category",
            F.when(F.col("credit_score") >= 750, "low_risk")
             .when(F.col("credit_score") >= 650, "medium_risk")
             .when(F.col("credit_score") >= 550, "high_risk")
             .otherwise("very_high_risk")
        ) 
        .withColumn("days_since_sanction", 
            F.datediff(F.current_date(), F.col("sanctioned_date"))
        )
    )
