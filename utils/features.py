# utils/features.py

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import StringIndexer
from functools import reduce

def build_features(spark: SparkSession, silver_path: str, gold_path: str):

    # 1) LOAD Silver
    df = spark.read.parquet(silver_path)

    # 2) COMPUTE MEDIAN AGE FOR IMPUTATION
    median_age = df.approxQuantile("Age", [0.5], 0.001)[0]

    # 3) IMPUTE & NULL FLAGS for Age
    df = (
        df
        .withColumn(
            "Age_missing",
            F.when(F.col("Age") < 0, 1).otherwise(0)
        )
        .withColumn(
            "Age_imputed",
            F.when(
                F.col("Age") < 0,
                F.lit(median_age).cast(DoubleType())
            )
            .otherwise(
                F.col("Age").cast(DoubleType())
            )
        )
    )

    # 4) ZERO-FILL clickstream features fe_1…fe_20 and add no_clicks flag
    for i in range(1, 21):
        df = df.withColumn(f"fe_{i}", F.coalesce(F.col(f"fe_{i}"), F.lit(0)))
    no_clicks_cond = reduce(
        lambda acc, i: acc & (F.col(f"fe_{i}") == 0),
        range(1, 21),
        F.lit(True)
    )
    df = df.withColumn("no_clicks", F.when(no_clicks_cond, 1).otherwise(0))

    # 5) DERIVE RATIOS & BINARY FLAGS
    eps = F.lit(1e-9)
    util_thresh = df.approxQuantile("Credit_Utilization_Ratio", [0.9], 0.001)[0]

    df = (
        df
        .withColumn("debt_to_income",      F.col("Outstanding_Debt") / (F.col("Annual_Income") + eps))
        .withColumn("loan_to_income",      F.col("total_loan_amount") / (F.col("Annual_Income") + eps))
        .withColumn("has_loans",           F.when(F.col("total_loan_amount") > 0, 1).otherwise(0))
        .withColumn("has_overdue",         F.when(F.col("total_overdue_amt")  > 0, 1).otherwise(0))
        .withColumn(
            "high_utilization",
            F.when(F.col("Credit_Utilization_Ratio") > util_thresh, 1).otherwise(0)
        )
    )

    # 6) SESSION_SUM aggregate
    fe_cols = [F.col(f"fe_{i}") for i in range(1, 21)]
    session_sum_expr = reduce(lambda a, b: a + b, fe_cols)
    df = df.withColumn("session_sum", session_sum_expr)

    # 7) TEMPORAL FEATURES
    df = (
        df
        .withColumn("month",        F.month("snapshot_date"))
        .withColumn("week_of_year", F.weekofyear("snapshot_date"))
        .withColumn(
            "days_since_first_loan",
            F.datediff("snapshot_date", "first_loan_date")
        )
    )

    # 8) CREDIT HISTORY BUCKETS (0–3)
    hist_qs = df.approxQuantile("Credit_History_Age", [0.25, 0.5, 0.75], 0.001)
    df = df.withColumn(
        "hist_bucket",
        F.when(F.col("Credit_History_Age") <= hist_qs[0], 0)
         .when(F.col("Credit_History_Age") <= hist_qs[1], 1)
         .when(F.col("Credit_History_Age") <= hist_qs[2], 2)
         .otherwise(3)
    )

    # 9) INDEX categorical columns to numeric
    for col in [
        "Type_of_Loan_clean",
        "Credit_Mix",
        "Payment_of_Min_Amount",
        "Payment_Behaviour",
        "hist_bucket"
    ]:
        idx_col = col + "_idx"
        df = (
            StringIndexer(inputCol=col, outputCol=idx_col, handleInvalid="keep")
            .fit(df)
            .transform(df)
        )

    # 10) FINAL SELECT of ML-ready features
    final = df.select(
        "Customer_ID",
        "snapshot_date",
        "Age_imputed", "Age_missing",
        "debt_to_income", "loan_to_income",
        "has_loans", "has_overdue", "high_utilization",
        "session_sum",
        *[f"fe_{i}" for i in range(1, 21)],
        "no_clicks",
        "days_since_first_loan",
        "hist_bucket_idx",
        "Type_of_Loan_clean_idx",
        "Credit_Mix_idx",
        "Payment_of_Min_Amount_idx",
        "Payment_Behaviour_idx"
    )

    # 11) WRITE out Gold partitioned by snapshot_date
    final.write \
         .mode("overwrite") \
         .partitionBy("snapshot_date") \
         .parquet(gold_path)


