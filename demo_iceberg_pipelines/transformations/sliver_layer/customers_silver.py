from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()


@dp.temporary_view
def customers_silver() -> DataFrame:
    """Validate and clean bronze data"""
    df = spark.table("customers_bronze")
    return df.filter(
        (F.col("customer_id").isNotNull()) & 
        (F.col("kyc_status").isin("verified", "pending", "rejected"))
    ).withColumn("email_domain", F.split(F.col("email"), "@")[1])