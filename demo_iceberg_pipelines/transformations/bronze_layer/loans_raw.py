from pyspark import pipelines as dp
from pyspark.sql import SparkSession, DataFrame
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from schemas.schemas import LOANS_BRONZE_SCHEMA


spark = (
    SparkSession.active()
)



dp.create_streaming_table("loans_bronze")



@dp.append_flow(target="loans_bronze")
def loans_incr_flow()->DataFrame : 
    loans_df = (
        spark
        .readStream
        .schema(LOANS_BRONZE_SCHEMA)
        .option("header" , "true")
        .option("maxFilesPerTrigger", 50)
        .csv("s3a://<YOUR_S3_BUCKET_NAME>/loans/")
    )

    return loans_df
