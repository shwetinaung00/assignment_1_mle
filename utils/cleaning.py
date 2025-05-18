# utils/cleaning.py

def clean_and_join(spark, bronze_paths: dict, silver_path: str):
    """
    Read raw Parquet tables from the Bronze layer, apply cleaning and 
    simple joins, then write out a unified Silver-layer Parquet table.

    Parameters
    ----------
    spark : SparkSession
        Your active Spark session.
    bronze_paths : dict
        Dictionary of Bronze parquet locations, e.g.:
          {
            'clickstream': 'datamart/bronze/clickstream',
            'attributes':  'datamart/bronze/attributes',
            'financials':  'datamart/bronze/financials'
          }
    silver_path : str
        Output path for the cleaned/joined Silver table, e.g.
        'datamart/silver/feature_store'
    """
    # TODO: 
    #   1. Read each Bronze table via spark.read.parquet(bronze_paths['...'])
    #   2. Drop duplicates / fill nulls
    #   3. Join them on a common key (e.g. user_id)
    #   4. Write the result to silver_path
    pass
