# utils/cleaning.py

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def clean_and_join(
    spark: SparkSession,
    bronze_paths: dict,
    silver_path: str
):
    """
    Read Bronze parquet tables, apply cleaning and simple feature engineering,
    join them into one Silver-layer table, and write out as Parquet.

    Parameters
    ----------
    spark : SparkSession
    bronze_paths : dict
        {
          'clickstream': 'datamart/bronze/clickstream',
          'attributes':  'datamart/bronze/attributes',
          'financials':  'datamart/bronze/financials',
          'loan_daily':  'datamart/bronze/loan_daily'
        }
    silver_path : str
        e.g. 'datamart/silver/feature_store'
    """
    # 1. Load Bronze tables
    click_df = spark.read.parquet(bronze_paths['clickstream'])
    attr_df  = spark.read.parquet(bronze_paths['attributes'])
    fin_df   = spark.read.parquet(bronze_paths['financials'])
    loan_df  = spark.read.parquet(bronze_paths['loan_daily'])

    # 2. Clean clickstream: drop duplicates
    click_clean = click_df.dropDuplicates()

    # 3. Clean attributes: drop dupes & fill nulls
    attr_clean = (
        attr_df
        .dropDuplicates()
        .na.fill({
            'Age': 0,
            'Occupation': 'unknown'
        })
    )

    # 4. Clean financials
    fin_clean = (
        fin_df
        .dropDuplicates()
        # Recode missing or multi‐loan entries
        .withColumn(
            'Type_of_Loan_clean',
            F.when(
                F.col('Type_of_Loan').isNull() |
                (F.col('Type_of_Loan') == 'Not Specified'),
                F.lit('Unknown')
            )
            .when(
                F.col('Type_of_Loan').contains(',') |
                F.col('Type_of_Loan').contains(' and '),
                F.lit('Multiple')
            )
            .otherwise(F.col('Type_of_Loan'))
        )
        # Normalize casing: “personal loan” → “Personal Loan”
        .withColumn(
            'Type_of_Loan_clean',
            F.initcap(F.lower(F.col('Type_of_Loan_clean')))
        )
        # Clamp negative numeric anomalies
        .withColumn(
            'Num_Bank_Accounts',
            F.when(F.col('Num_Bank_Accounts') < 0, 0)
             .otherwise(F.col('Num_Bank_Accounts'))
        )
        .withColumn(
            'Delay_from_due_date',
            F.when(F.col('Delay_from_due_date') < 0, 0)
             .otherwise(F.col('Delay_from_due_date'))
        )
        .withColumn(
            'Interest_Rate',
            F.when(F.col('Interest_Rate') < 0, 0)
             .otherwise(F.col('Interest_Rate'))
        )
        # Fill remaining numeric nulls sensibly
        .na.fill({
            'Annual_Income':             0.0,
            'Monthly_Inhand_Salary':     0.0,
            'Num_Credit_Card':           0,
            'Num_of_Loan':               0,
            'Outstanding_Debt':          0.0,
            'Credit_Utilization_Ratio':  0.0,
            'Num_Credit_Inquiries':      0,
            'Delay_from_due_date':       0
        })
    )

    # 5. Aggregate loan‐daily up to user snapshot
    loan_agg = (
        loan_df
        .groupBy('Customer_ID', 'snapshot_date')
        .agg(
            F.sum('loan_amt').alias('total_loan_amount'),
            F.avg('overdue_amt').alias('avg_overdue_amt'),
            F.max('balance').alias('max_balance'),
            F.count('*').alias('num_installments')
        )
    )

    # 6. Join all cleaned tables
    joined = (
        attr_clean.alias('a')
        .join(
            fin_clean.alias('f'),
            on=['Customer_ID', 'snapshot_date'],
            how='left'
        )
        .join(
            click_clean.alias('c'),
            on=['Customer_ID', 'snapshot_date'],
            how='left'
        )
        .join(
            loan_agg.alias('l'),
            on=['Customer_ID', 'snapshot_date'],
            how='left'
        )
    )

    # 7. Write out the Silver-layer table
    joined.write.mode("overwrite").parquet(silver_path)
