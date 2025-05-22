# utils/ingestion.py

from pyspark.sql import SparkSession

def ingest_raw(spark: SparkSession, csv_path: str, output_path: str):
    
    df = (
        spark.read
             .option("header", True)
             .option("inferSchema", True)     
             .csv(csv_path)
    )
    df.write.mode("overwrite").parquet(output_path)



