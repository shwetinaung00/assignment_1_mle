# utils/cleaning.py

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import IntegerType, DoubleType

def clean_and_join(spark: SparkSession, bronze_paths: dict, silver_path: str):

    
    # --- 1) LOAD & CAST ---
    click_df = (
        spark.read.parquet(bronze_paths['clickstream'])
             .withColumn("snapshot_date", F.to_date("snapshot_date", "yyyy-MM-dd"))
    )
    attr_df = (
        spark.read.parquet(bronze_paths['attributes'])
             .withColumn("snapshot_date", F.to_date("snapshot_date", "yyyy-MM-dd"))
             .withColumn("Age", F.col("Age").cast(IntegerType()))
             .withColumn(
                 "Occupation",
                 F.when(F.col("Occupation").rlike("^_+$"), None)
                  .otherwise(F.col("Occupation"))
             )
    )
    fin_df = (
        spark.read.parquet(bronze_paths['financials'])
             .withColumn("snapshot_date", F.to_date("snapshot_date", "yyyy-MM-dd"))
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
             # parse "X Years and Y Months" → years + months/12
             .withColumn(
                 "Credit_History_Age",
                 F.regexp_extract("Credit_History_Age", r"(\d+)\s*Years", 1).cast(DoubleType()) +
                 F.regexp_extract("Credit_History_Age", r"(\d+)\s*Months",1).cast(DoubleType())/12
             )
             .withColumn("Total_EMI_per_month",   F.col("Total_EMI_per_month").cast(DoubleType()))
             .withColumn("Amount_invested_monthly", F.col("Amount_invested_monthly").cast(DoubleType()))
             .withColumn("Monthly_Balance",       F.col("Monthly_Balance").cast(DoubleType()))
             .withColumn(
                 "Credit_Mix",
                 F.when(F.col("Credit_Mix").isin("_",""), None)
                  .otherwise(F.col("Credit_Mix"))
             )
    )
    loan_df = (
        spark.read.parquet(bronze_paths['loan_daily'])
             .withColumn("snapshot_date", F.to_date("snapshot_date", "yyyy-MM-dd"))
    )

    # --- 2) PER-SNAPSHOT AGGREGATES ---
    loan_agg = (
        loan_df
        .groupBy("Customer_ID","snapshot_date")
        .agg(
            F.sum("loan_amt").alias("total_loan_amount"),
            F.sum("overdue_amt").alias("total_overdue_amt"),
            F.max("balance").alias("max_balance"),
            F.count("*").alias("num_installments")
        )
    )

    # --- 3) FIRST LOAN DATE PER CUSTOMER ---
    first_loan_date_df = (
        loan_df
        .groupBy("Customer_ID")
        .agg(F.min("snapshot_date").alias("first_loan_date"))
    )

    # --- 4) BASIC CLEANING ---
    click_clean = click_df.dropDuplicates()
    attr_clean = (
        attr_df
        .dropDuplicates()
        .na.fill({"Occupation":"unknown", "Age":-1})
    )
    fin_clean = (
        fin_df
        .dropDuplicates()
        .withColumn(
            "Type_of_Loan_clean",
            F.when(F.col("Type_of_Loan").isNull() |
                   (F.col("Type_of_Loan")=="Not Specified"), "Unknown")
             .when(F.col("Type_of_Loan").contains(",") |
                   F.col("Type_of_Loan").contains(" and "), "Multiple")
             .otherwise(F.col("Type_of_Loan"))
        )
        .withColumn("Type_of_Loan_clean", F.initcap(F.lower("Type_of_Loan_clean")))
        .withColumn("Num_Bank_Accounts",
                    F.when(F.col("Num_Bank_Accounts")<0,0).otherwise("Num_Bank_Accounts"))
        .na.fill({
            "Annual_Income":0.0,
            "Monthly_Inhand_Salary":0.0,
            "Num_of_Loan":0,
            "Outstanding_Debt":0.0,
            "Credit_Utilization_Ratio":0.0,
            "Num_Credit_Inquiries":0,
            "Delay_from_due_date":0,
            "Credit_Mix":"unknown"
        })
    )

    # --- 5) JOIN EVERYTHING ---
    joined = (
        attr_clean.alias("a")
        .join(fin_clean.alias("f"),["Customer_ID","snapshot_date"],how="left")
        .join(click_clean.alias("c"),["Customer_ID","snapshot_date"],how="left")
        .join(loan_agg.alias("l"),   ["Customer_ID","snapshot_date"],how="left")
        .join(first_loan_date_df.alias("d"),["Customer_ID"],how="left")
    )

    # --- 6) WRITE SILVER ---
    joined.write.mode("overwrite").parquet(silver_path)

