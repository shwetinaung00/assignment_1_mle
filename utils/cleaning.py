# utils/cleaning.py

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, DoubleType

def clean_and_join(
    spark: SparkSession,
    bronze_paths: dict,
    silver_path: str
):
    """
    Read Bronze parquet tables, cast types, apply cleaning and recoding,
    join into one Silver-layer table, and write out as Parquet.
    """
    # 1. Load and cast Bronze tables
    click_df = (
        spark.read.parquet(bronze_paths['clickstream'])
             .withColumn("snapshot_date", F.to_date("snapshot_date", "yyyy-MM-dd"))
    )

    attr_df = (
        spark.read.parquet(bronze_paths['attributes'])
             .withColumn("snapshot_date", F.to_date("snapshot_date", "yyyy-MM-dd"))
             .withColumn("Age", F.col("Age").cast(IntegerType()))
             # Null‐out underscore placeholders
             .withColumn(
                 "Occupation",
                 F.when(F.col("Occupation").rlike("^_+$"), None)
                  .otherwise(F.col("Occupation"))
             )
    )

    fin_df = (
        spark.read.parquet(bronze_paths['financials'])
             .withColumn("snapshot_date", F.to_date("snapshot_date", "yyyy-MM-dd"))
             # Cast numerics
             .withColumn("Annual_Income",         F.col("Annual_Income").cast(DoubleType()))
             .withColumn("Monthly_Inhand_Salary", F.col("Monthly_Inhand_Salary").cast(DoubleType()))
             .withColumn("Num_Bank_Accounts",     F.col("Num_Bank_Accounts").cast(IntegerType()))
             .withColumn("Num_Credit_Card",       F.col("Num_Credit_Card").cast(IntegerType()))
             .withColumn("Interest_Rate",         F.col("Interest_Rate").cast(DoubleType()))
             .withColumn("Num_of_Loan",           F.col("Num_of_Loan").cast(IntegerType()))
             .withColumn("Delay_from_due_date",   F.col("Delay_from_due_date").cast(IntegerType()))
             .withColumn("Num_of_Delayed_Payment",F.col("Num_of_Delayed_Payment").cast(IntegerType()))
             .withColumn("Changed_Credit_Limit",  F.col("Changed_Credit_Limit").cast(IntegerType()))
             .withColumn("Num_Credit_Inquiries",  F.col("Num_Credit_Inquiries").cast(IntegerType()))
             .withColumn("Outstanding_Debt",      F.col("Outstanding_Debt").cast(DoubleType()))
             .withColumn("Credit_Utilization_Ratio", F.col("Credit_Utilization_Ratio").cast(DoubleType()))
             # Parse "X Years and Y Months" → numeric years + months/12
                .withColumn(
                    "Credit_History_Age",
                    (
                    F.regexp_extract("Credit_History_Age", r"(\d+)\s*Years", 1).cast(DoubleType())
                    +
                    F.regexp_extract("Credit_History_Age", r"(\d+)\s*Months",1).cast(DoubleType())/12
                    )
                )

             .withColumn("Total_EMI_per_month",   F.col("Total_EMI_per_month").cast(DoubleType()))
             .withColumn("Amount_invested_monthly", F.col("Amount_invested_monthly").cast(DoubleType()))
             .withColumn("Monthly_Balance",       F.col("Monthly_Balance").cast(DoubleType()))
             # Null‐out bad Credit_Mix placeholders
             .withColumn(
                 "Credit_Mix",
                 F.when(F.col("Credit_Mix").isin("_", ""), None)
                  .otherwise(F.col("Credit_Mix"))
             )
    )

    loan_df = (
        spark.read.parquet(bronze_paths['loan_daily'])
             .withColumn("snapshot_date", F.to_date("snapshot_date", "yyyy-MM-dd"))
    )

    # 2. Clean clickstream: dedupe
    click_clean = click_df.dropDuplicates()

    # 3. Clean attributes: dedupe, then fill nulls
    attr_clean = (
        attr_df
        .dropDuplicates()
        .na.fill({
            "Occupation": "unknown",
            "Age":        -1
        })
    )

    # 4. Clean financials: dedupe, recode loan types, clamp, fill nulls
    fin_clean = (
        fin_df
        .dropDuplicates()
        .withColumn(
            "Type_of_Loan_clean",
            F.when(
                F.col("Type_of_Loan").isNull() |
                (F.col("Type_of_Loan") == "Not Specified"),
                F.lit("Unknown")
            )
            .when(
                F.col("Type_of_Loan").contains(",") |
                F.col("Type_of_Loan").contains(" and "),
                F.lit("Multiple")
            )
            .otherwise(F.col("Type_of_Loan"))
        )
        .withColumn(
            "Type_of_Loan_clean",
            F.initcap(F.lower(F.col("Type_of_Loan_clean")))
        )
        # Clamp negatives
        .withColumn(
            "Num_Bank_Accounts",
            F.when(F.col("Num_Bank_Accounts") < 0, 0)
             .otherwise(F.col("Num_Bank_Accounts"))
        )
        .withColumn(
            "Delay_from_due_date",
            F.when(F.col("Delay_from_due_date") < 0, 0)
             .otherwise(F.col("Delay_from_due_date"))
        )
        .withColumn(
            "Interest_Rate",
            F.when(F.col("Interest_Rate") < 0, 0)
             .otherwise(F.col("Interest_Rate"))
        )
        # Fill remaining nulls
        .na.fill({
            "Annual_Income":            0.0,
            "Monthly_Inhand_Salary":    0.0,
            "Num_Credit_Card":          0,
            "Num_of_Loan":              0,
            "Outstanding_Debt":         0.0,
            "Credit_Utilization_Ratio": 0.0,
            "Num_Credit_Inquiries":     0,
            "Delay_from_due_date":      0,
            "Credit_Mix":               "unknown"
        })
    )

    # 5. Aggregate loan_daily up to (Customer_ID, snapshot_date)
    loan_agg = (
        loan_df
        .groupBy("Customer_ID", "snapshot_date")
        .agg(
            F.sum("loan_amt").alias("total_loan_amount"),
            F.avg("overdue_amt").alias("avg_overdue_amt"),
            F.max("balance").alias("max_balance"),
            F.count("*").alias("num_installments")
        )
    )

    # 6. Join all cleaned tables
    joined = (
        attr_clean.alias("a")
        .join(fin_clean.alias("f"), ["Customer_ID", "snapshot_date"], how="left")
        .join(click_clean.alias("c"), ["Customer_ID", "snapshot_date"], how="left")
        .join(loan_agg.alias("l"),      ["Customer_ID", "snapshot_date"], how="left")
    )

    # 7. Write out the Silver-layer table
    joined.write.mode("overwrite").parquet(silver_path)
