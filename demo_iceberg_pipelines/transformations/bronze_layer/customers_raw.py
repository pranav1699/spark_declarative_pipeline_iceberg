from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from schemas.schemas import CUSTOMERS_BRONZE_SCHEMA

spark = SparkSession.active()



dp.create_streaming_table(name="customers_bronze")



@dp.append_flow(target="customers_bronze")
def customers_incr_flow() -> DataFrame:
    customers_df = (
        spark
        .readStream
        .schema(CUSTOMERS_BRONZE_SCHEMA)
        .option("header", "true")
        .option("maxFilesPerTrigger", 50)
        .csv("s3a://<YOUR_S3_BUCKET_NAME>/customers/*.csv")
    )

    return customers_df




