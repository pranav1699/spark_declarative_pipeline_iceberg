from pyspark import pipelines as dp
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = (
    SparkSession.active()
)




@dp.temporary_view
def deposits_silver() -> DataFrame:
    df = spark.table("deposits_bronze")
    return (df 
        .withColumn("days_to_maturity",
            F.datediff(F.col("maturity_date"), F.current_date())
        ) 
        .withColumn("maturity_status",
            F.when(F.col("days_to_maturity") <= 0, "matured")
             .when(F.col("days_to_maturity") <= 30, "maturing_soon")
             .otherwise("active")
        ) 
        .withColumn("annual_interest_earned",
            F.col("principal_amount") * F.col("interest_rate") / 100
        )
    )
