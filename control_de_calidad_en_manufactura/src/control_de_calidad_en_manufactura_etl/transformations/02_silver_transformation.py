"""
The purpose of this script is to build the second layer of our `Medallion`
architecture for Manufacturing Quality Control. It takes the raw information 
from the `Bronze` layer, cleans it using declarative data quality rules 
(`Expectations`), and merges the streams using `Watermarks` to create an
enriched, unified dataset ready for machine learning defect detection.
"""

# ----------------- Imports -----------------

import pyspark.pipelines as dp

# Functions for data manipulation and constraints
from pyspark.sql.functions import col, expr, to_timestamp

# Centralized data quality rules repository
from rules import get_rules


# ----------------- Events: inspections (quarantine & typing) -----------------


insp_quarantine_table_name  = "silver_quarantine_inspections"
insp_quarantine_comment = """
This managed table acts as the **`Dead Letter Queue` (`DLQ`)** for unit
inspections within the **`silver`** layer. It captures all events
from `bronze_inspections` that failed to pass the established business
and data quality rules (`Expectations`). It preserves the invalid records
for auditing and troubleshooting by the `QA` team without
halting the main pipeline.
"""

insp_bronze_source = "bronze_inspections"
insp_tmp_eval_name = "tmp_eval_inspections"
insp_clean_view_name = "vw_clean_inspections"
insp_quarantine_flow_name = "flow_quarantine_insp"

insp_rule_dict = get_rules("inspections")
insp_rule_constraints = insp_rule_dict.values()
insp_combined_rules = " AND ".join(insp_rule_constraints)
insp_quarantine_expr = "NOT " + "(" + insp_combined_rules + ")"

dp.create_streaming_table(
    name = insp_quarantine_table_name,
    comment = insp_quarantine_comment
)

@dp.table(name = insp_tmp_eval_name, temporary = True)
@dp.expect_all(insp_rule_dict)
def eval_inspections():
    """
    Reads raw inspections, applies type casting to dates, and evaluates
    data quality rules.

    The `@dp.expect_all` decorator logs the metrics in the user interface.

    We also dynamically add a `boolean` flag (`is_quarantined`) to route
    records later.
    """
    df_raw = spark.readStream.table(insp_bronze_source)
    df_typed = df_raw.withColumn("timestamp", to_timestamp(col("timestamp")))
    df_evaluated = df_typed.withColumn("is_quarantined", expr(insp_quarantine_expr))

    return df_evaluated

@dp.append_flow(target = insp_quarantine_table_name, name = insp_quarantine_flow_name)
def quarantine_inspections():
    """
    Filters the evaluated inspections and appends **only** the invalid ones
    (`is_quarantined = true`) to the physical `DLQ` table.

    We drop the temporary flag before writing.
    """
    df_evaluated = spark.readStream.table(insp_tmp_eval_name)
    df_invalid = df_evaluated.filter("is_quarantined = true").drop("is_quarantined")

    return df_invalid

@dp.view(name = insp_clean_view_name)
def clean_inspections():
    """
    Provides a clean, filtered stream of valid inspections
    (`is_quarantined = false`).

    This view will be used in the next step to perform the stream-stream
    join with the labels.
    """
    df_evaluated = spark.readStream.table(insp_tmp_eval_name)
    df_valid = df_evaluated.filter("is_quarantined = false").drop("is_quarantined")

    return df_valid



# ----------------- Events: labels (quarantine & typing) -----------------


lbl_quarantine_table_name = "silver_quarantine_labels"
lbl_quarantine_comment = """
This managed table acts as the **`Dead Letter Queue` (`DLQ`)** for delayed
defect feedback within the **`silver`** layer. It captures all events from
`bronze_labels` that failed to pass the established quality rules.
"""

lbl_bronze_source = "bronze_labels"
lbl_tmp_eval_name = "tmp_eval_labels"
lbl_clean_view_name = "vw_clean_labels"
lbl_quarantine_flow_name = "flow_quarantine_lbl"

lbl_rules_dict = get_rules("labels")
lbl_rule_constraints = lbl_rules_dict.values()
lbl_combined_rules = " AND ".join(lbl_rule_constraints)
lbl_quarantine_expr = "NOT " + "(" + lbl_combined_rules + ")"

dp.create_streaming_table(
    name = lbl_quarantine_table_name,
    comment = lbl_quarantine_comment
)

@dp.table(name = lbl_tmp_eval_name, temporary = True)
@dp.expect_all(lbl_rules_dict)
def eval_labels():
    """
    Reads raw defect labels, applies type casting to dates, and evaluates data
    quality rules.
    """
    df_raw = spark.readStream.table(lbl_bronze_source)
    df_typed = df_raw.withColumn("label_available_date", to_timestamp(col("label_available_date")))
    df_evaluated = df_typed.withColumn("is_quarantined", expr(lbl_quarantine_expr))

    return df_evaluated

@dp.append_flow(target = lbl_quarantine_table_name, name = lbl_quarantine_flow_name)
def quarantine_labels():
    """
    Filters the evaluated labels and appends **only** the invalid ones.
    """
    df_evaluated = spark.readStream.table(lbl_tmp_eval_name)
    df_invalid = df_evaluated.filter("is_quarantined = true").drop("is_quarantined")

    return df_invalid

@dp.view(name = lbl_clean_view_name)
def clean_labels():
    """
    Provides a clean, filtered stream of valid labels.
    """
    df_evaluated = spark.readStream.table(lbl_tmp_eval_name)
    df_valid = df_evaluated.filter("is_quarantined = false").drop("is_quarantined")

    return df_valid


# ----------------- Enriched events: stream-stream join -----------------


silver_defect_events_table = "silver_defect_events"
silver_defect_events_comment = """
This managed table is the **unified core** of the **`silver`** layer. It
contains enriched unit inspections joined with their corresponding
defect labels. It uses **`Watermarks`** to handle delayed feedback and state
management, ensuring no data leakage and providing a point-in-time accurate
dataset for machine learning training.
"""

# According to the usecase provided, labels take between 1 and 30 days to become available.
# We give it a 35-day watermark to be safe and ensure we don't drop late-arriving labels.
insp_watermark_delay = "35 days"
labels_watermark_delay = "1 day"

@dp.table(name = silver_defect_events_table, comment = silver_defect_events_comment)
def silver_events_join():
    """
    Executes a stateful stream-stream left join between clean inspections and labels.

    Records without a matching label within the watermark window are retained with
    `is_defective = null`, representing units still awaiting laboratory confirmation.
    """
    df_insp = spark.readStream.table(insp_clean_view_name).withWatermark("timestamp", insp_watermark_delay)
    df_lbl = spark.readStream.table(lbl_clean_view_name).withWatermark("label_available_date", labels_watermark_delay)

    join_on = [
        # Match by unit identifier
        col("insp.unit_id") == col("lbl.unit_id"),

        # Label must arrive after the inspection occurred
        col("lbl.label_available_date") > col("insp.timestamp"),

        # Label must arrive within the allowed 35-day delayed feedback window
        col("lbl.label_available_date") <= col("insp.timestamp") + expr(f"INTERVAL {insp_watermark_delay}")
    ]

    df_joined = df_insp.alias("insp").join(df_lbl.alias("lbl"), on = join_on, how = "leftOuter")

    # Select final columns explicitly to avoid ambiguity.
    return df_joined.select(
        col("insp.*"),
        col("lbl.is_defective"),
        col("lbl.label_available_date")
    )