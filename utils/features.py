from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from functools import reduce

# utils/features.py

def build_features(spark: SparkSession, silver_path: str, gold_path: str):
    """
    Read the cleaned Silver table, compute derived features,
    and write out the final Gold-layer table.
    """
    # 1. Load Silver table
    df = spark.read.parquet(silver_path)

    # 2. Precompute quantile-based thresholds
    median_age = df.approxQuantile("Age", [0.5], 0.001)[0]
    util_thresh = df.approxQuantile("Credit_Utilization_Ratio", [0.9], 0.001)[0]
    hist_q25, hist_q50, hist_q75 = df.approxQuantile("Credit_History_Age", [0.25, 0.5, 0.75], 0.001)

    eps = F.lit(1e-9)

    # 3. Clickstream nulls → zeros, and no_clicks flag
    # Fill any fe_i nulls with 0
    for i in range(1, 21):
        df = df.withColumn(f"fe_{i}", F.coalesce(F.col(f"fe_{i}"), F.lit(0)))
    # Flag rows where all fe_1…fe_20 == 0
    no_clicks_cond = reduce(
        lambda acc, i: acc & (F.col(f"fe_{i}") == 0),
        range(1, 21),
        F.lit(True)
    )
    df = df.withColumn("no_clicks", F.when(no_clicks_cond, 1).otherwise(0))

    # 4. Behavioral: sum of clickstream features
    fe_cols = [F.col(f"fe_{i}") for i in range(1, 21)]
    session_sum_expr = reduce(lambda a, b: a + b, fe_cols)

    # 5. Window for computing first loan date
    first_loan_window = Window.partitionBy("Customer_ID")

    # 6. Build features
    features = (
        df
        # Impute Age and flag missing
        .withColumn("Age_missing", F.when(F.col("Age") < 0, 1).otherwise(0))
        .withColumn("Age_imputed", F.when(F.col("Age") < 0, median_age).otherwise(F.col("Age")))

        # Ratios
        .withColumn("debt_to_income", F.col("Outstanding_Debt") / (F.col("Annual_Income") + eps))
        .withColumn("loan_to_income", F.col("total_loan_amount") / (F.col("Annual_Income") + eps))
        .withColumn("inquiry_per_year", F.col("Num_Credit_Inquiries") / (F.col("Credit_History_Age") + eps))

        # Binary flags
        .withColumn("has_loans", F.when(F.col("total_loan_amount") > 0, 1).otherwise(0))
        .withColumn("has_overdue", F.when(F.col("avg_overdue_amt") > 0, 1).otherwise(0))
        .withColumn("high_utilization", F.when(F.col("Credit_Utilization_Ratio") > util_thresh, 1).otherwise(0))

        # Behavioral aggregate
        .withColumn("session_sum", session_sum_expr)

        # Temporal features
        .withColumn("month", F.month("snapshot_date"))
        .withColumn("week_of_year", F.weekofyear("snapshot_date"))
        .withColumn("first_loan_date", F.min("snapshot_date").over(first_loan_window))
        .withColumn("days_since_first_loan", F.datediff("snapshot_date", "first_loan_date"))

        # Credit history buckets
        .withColumn(
            "hist_bucket",
            F.when(F.col("Credit_History_Age") <= hist_q25, "<=25th")
             .when(F.col("Credit_History_Age") <= hist_q50, "25-50th")
             .when(F.col("Credit_History_Age") <= hist_q75, "50-75th")
             .otherwise(">75th")
        )
    )

    # 7. Select final feature set including raw and new features
    final = features.select(
        "Customer_ID",
        "snapshot_date",
        "Age_imputed",
        "Age_missing",
        "debt_to_income",
        "loan_to_income",
        "inquiry_per_year",
        "has_loans",
        "has_overdue",
        "high_utilization",
        "session_sum",
        # raw clickstream
        *[f"fe_{i}" for i in range(1, 21)],
        # no_clicks flag
        "no_clicks",
        "month",
        "week_of_year",
        "days_since_first_loan",
        "hist_bucket",
        "Type_of_Loan_clean",
        "Credit_Mix",
        "Payment_of_Min_Amount",
        "Payment_Behaviour"
    )

    # 8. Write out Gold-layer feature table
    final.write.mode("overwrite").parquet(gold_path)

