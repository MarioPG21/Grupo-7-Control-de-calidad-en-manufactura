"""
The purpose of this script is to build the behavioral feature table
for the `Feature Store`. It reads clean events from the `Silver` layer
and calculates time-windowed aggregations per machine (e.g., vibration
trends, tool wear, production speed stability).

This table is keyed by `machine_id` and is designed to be published
to the `Online Feature Store` for real-time inference lookups.

Architecture note: We use a batch processing approach with rolling windows 
(`Window.partitionBy`) to guarantee point-in-time correctness. This ensures
every single inspection retains its exact historical context without data
leakage from the future.
"""


# ---------------- Imports ----------------

import pyspark.pipelines as dp
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    lit,
    max,
    min,
    stddev
)


# ---------------- Configuration variables and constants ----------------

silver_events_source = "silver_defect_events"
EPSILON = 1e-6  # Small value used to avoid division by zero


# ---------------- Final feature table: rolling window aggregations ----------------

gold_aggregations_table_name = "gold_machine_aggregations"
gold_aggregations_comment = """
This managed table acts as the **behavioral feature table** in the `Feature
Store`. It consolidates time-windowed aggregations per machine across three
temporal resolutions (1 hour, 8 hours, 24 hours) into a single row per
machine per inspection timestamp.

It is keyed by `machine_id` and is designed to be published to the `Online
Feature Store` for low-latency lookups during real-time inference.
"""

gold_aggregations_table_properties = {"delta.enableChangeDataFeed": "true"}
gold_aggregations_schema = """
    machine_id STRING NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    count_units_1h BIGINT,
    avg_vibration_1h DOUBLE,
    max_tool_wear_1h DOUBLE,
    count_units_8h BIGINT,
    avg_vibration_8h DOUBLE,
    max_vibration_8h DOUBLE,
    avg_temperature_8h DOUBLE,
    stddev_cycle_time_8h DOUBLE,
    count_units_24h BIGINT,
    avg_vibration_24h DOUBLE,
    vibration_1h_vs_8h_ratio DOUBLE,
    CONSTRAINT gold_machine_aggregations_pk PRIMARY KEY (machine_id, timestamp TIMESERIES)
"""


@dp.table(
    name = gold_aggregations_table_name,
    comment = gold_aggregations_comment,
    table_properties = gold_aggregations_table_properties,
    schema = gold_aggregations_schema
)
def gold_machine_aggregations():
    """
    Computes rolling window aggregations in a single pass using batch processing.
    """
    df_silver = spark.read.table(silver_events_source)

    # Convert timestamp to milliseconds for sub-second precision.
    df = df_silver.withColumn("ts_ms", (col("timestamp").cast("double") * 1000).cast("long"))

    # Define rolling windows in milliseconds.
    # We use -1 as the upper bound to intentionally exclude the current
    # inspection from its own aggregation window, preventing data leakage.
    w_1h  = Window.partitionBy("machine_id").orderBy("ts_ms").rangeBetween(-3_600_000, -1)
    w_8h  = Window.partitionBy("machine_id").orderBy("ts_ms").rangeBetween(-28_800_000, -1)
    w_24h = Window.partitionBy("machine_id").orderBy("ts_ms").rangeBetween(-86_400_000, -1)

    # Compute all aggregations on the fly.
    df_agg = df.select(
        col("machine_id"),
        col("timestamp"),

        # 1 hour (Short-term immediate state)
        count("unit_id").over(w_1h).alias("count_units_1h"),
        avg("vibration_mm_s").over(w_1h).alias("avg_vibration_1h"),
        max("tool_wear_pct").over(w_1h).alias("max_tool_wear_1h"),

        # 8 hours (Shift-level state)
        count("unit_id").over(w_8h).alias("count_units_8h"),
        avg("vibration_mm_s").over(w_8h).alias("avg_vibration_8h"),
        max("vibration_mm_s").over(w_8h).alias("max_vibration_8h"),
        avg("temperature_celsius").over(w_8h).alias("avg_temperature_8h"),
        stddev("cycle_time_s").over(w_8h).alias("stddev_cycle_time_8h"),

        # 24 hours (Daily state)
        count("unit_id").over(w_24h).alias("count_units_24h"),
        avg("vibration_mm_s").over(w_24h).alias("avg_vibration_24h")
    )

    # Derived complex feature: Is the machine vibrating more NOW than its shift average?
    # This captures the "Machine degradation" concept drift introduced in 2025.
    df_final = df_agg.withColumn(
        "vibration_1h_vs_8h_ratio",
        coalesce(
            col("avg_vibration_1h") / (col("avg_vibration_8h") + lit(EPSILON)),
            lit(1.0)
        )
    )

    return df_final