# main.py

from pyspark.sql import SparkSession
from utils.ingestion import ingest_raw
from utils.cleaning import clean_and_join
from utils.features import build_features

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Assignment1_Pipeline") \
        .getOrCreate()

    # --- Bronze ingestion ---
    ingest_raw(
        spark,
        csv_path="data/feature_clickstream.csv",
        output_path="datamart/bronze/clickstream"
    )
    ingest_raw(
        spark,
        csv_path="data/features_attributes.csv",
        output_path="datamart/bronze/attributes"
    )
    ingest_raw(
        spark,
        csv_path="data/features_financials.csv",
        output_path="datamart/bronze/financials"
    )
    ingest_raw(
        spark,
        csv_path="data/lms_loan_daily.csv",
        output_path="datamart/bronze/loan_daily"
    )

    # --- Silver cleaning & join ---
    bronze_paths = {
        'clickstream': 'datamart/bronze/clickstream',
        'attributes':  'datamart/bronze/attributes',
        'financials':  'datamart/bronze/financials',
        'loan_daily':  'datamart/bronze/loan_daily'
    }
    clean_and_join(
        spark,
        bronze_paths=bronze_paths,
        silver_path="datamart/silver/feature_store"
    )

    # --- Gold feature engineering ---
    build_features(
        spark,
        silver_path="datamart/silver/feature_store",
        gold_path="datamart/gold/feature_store"
    )

    spark.stop()
