"""
The purpose of this script is to build the `Spine` table of our `Medallion`
architecture. It takes the clean events from the `Silver` layer and prepares
the foundational dataset (entities, timestamps, and real-time features) used
by the `Feature Store` to orchestrate point-in-time correct joins during
training and inference.
"""


# ---------------- Imports ----------------

import pyspark.pipelines as dp
from pyspark.sql.functions import col, date_format, to_timestamp


# ---------------- Feature engineering (`Spine` creation) ----------------

gold_spine_table_name = "gold_defect_spine"
gold_spine_comment = """
This managed table acts as the `Spine DataFrame` for machine learning.
It contains the primary keys (`unit_id`), event times (`timestamp`),
real-time manufacturing features (sensor readings), and the target label (`is_defective`).
"""

silver_events_source = "silver_defect_events"


@dp.table(name = gold_spine_table_name, comment = gold_spine_comment)
def gold_defect_spine():
    """
    Reads the enriched inspections to build the machine learning spine.

    Passes through the primary keys, event timestamps, and real-time
    features necessary for the `Feature Store`'s `PiT` lookups.
    """
    df_events = spark.readStream.table(silver_events_source)

    # Force an irreversible physical transformation to bypass the optimizer:
    # we convert the date to a string with an explicit format and back to a timestamp.
    # This guarantees the removal of the hidden watermark metadata from
    # "label_available_date".
    df_events = df_events.withColumn(
        "label_available_date",
        to_timestamp(date_format(col("label_available_date"), "yyyy-MM-dd HH:mm:ss.SSS"))
    )

    # Select core fields strictly related to the inspection (the spine)
    df_spine = df_events.select(
        # Primary keys & event time column
        col("unit_id"),
        col("machine_id"),
        col("timestamp"),

        # Labels
        col("is_defective"),
        col("label_available_date"),

        # Real-time features (available directly from the machine sensors at serving time)
        # Contextual / Process parameters
        col("line_id"),
        col("shift"),
        col("material_batch_id"),
        col("operator_experience_yrs"),
        
        # Sensor readings (Physical metrics)
        col("temperature_celsius"),
        col("pressure_bar"),
        col("vibration_mm_s"),
        col("voltage_v"),
        col("current_ma"),
        col("humidity_pct"),
        col("particle_count_m3"),
        col("solder_thickness_um"),
        col("alignment_error_um"),
        col("optical_density"),
        
        # Machine / Process state metrics
        col("cycle_time_s"),
        col("tool_wear_pct"),
        col("time_since_maintenance_h"),
        col("production_speed_pct"),
        
        # Statistical process control (SPC)
        col("spc_xbar"),
        col("spc_range"),
        col("cumulative_defect_rate_shift")
    )

    return df_spine