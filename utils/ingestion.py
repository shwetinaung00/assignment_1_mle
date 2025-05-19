# utils/ingestion.py

from pyspark.sql import SparkSession

def ingest_raw(spark: SparkSession, csv_path: str, output_path: str):
    """
    Read a raw CSV with schema inference and write it out as Parquet.
    """
    df = (
        spark.read
             .option("header", True)
             .option("inferSchema", True)      # ← add this
             .csv(csv_path)
    )
    df.write.mode("overwrite").parquet(output_path)
