# utils/features.py

def build_features(spark, silver_path: str, gold_path: str):
    """
    Read your cleaned Silver table, compute aggregate features,
    and write out the final Gold-layer Parquet table.

    Parameters
    ----------
    spark : SparkSession
        Your active Spark session.
    silver_path : str
        Path to the Silver-layer Parquet (e.g. 'datamart/silver/feature_store').
    gold_path : str
        Output path for the Gold-layer Parquet (e.g. 'datamart/gold/feature_store').
    """
    # TODO:
    #   1. Read silver_df = spark.read.parquet(silver_path)
    #   2. Compute features with groupBy/agg (e.g. counts, averages)
    #   3. Write the features DataFrame to gold_path
    pass
