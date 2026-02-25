"""
`Bronze` layer ingestion: The purpose of this script is to establish the first
layer of our data architecture (`Medallion` architecture) known as the `Bronze`
layer. It handles the ingestion of raw files from the landing zone into
managed `Delta` tables using `Databricks Lakeflow Declarative Pipelines`.
It covers batch ingestion for master data (customers) and continuous
streaming ingestion via `Auto Loader` for events (transactions and labels).
"""

# ---------------- IMPORTS ----------------

# Object-oriented filesystem paths
from pathlib import Path

# Declarative pipelines
import pyspark.pipelines as dp

# Functions for metadata
from pyspark.sql.functions import col, current_timestamp

# ---------------- Configuration variables and constants ----------------

base_path = Path("/") / "Volumes" / "workspace" / "calidad_manufactura"
vol_landing_zone = base_path / "landing_zone"

# ---------------- Events ingestion: Inspections (streaming) ----------------

tx_table_name = "bronze_inspections"
tx_comment = """
This managed table acts as the historical and real-time repository for raw
quality inspection events within the **`bronze`** layer. It is continuously fed by
a streaming pipeline (`Auto Loader`), capturing every product inspection
in an append-only format. It preserves the original `.json` schema and includes
technical audit metadata (`ingestion_timestamp`, `source_file`) to ensure data
lineage and traceability.
"""

tx_flow_name = "bronze_tx_ingest_flow"

path_events_tx = vol_landing_zone / "events" / "inspections"

dp.create_streaming_table(name = tx_table_name, comment = tx_comment)


# A flow represents a data pipeline component that defines  how data is
# ingested, transformed, and routed. It acts as a logical grouping for
# the streaming operations.
#
# The decorator links the ingestion function to the target table. It instructs
# the  engine to continuously append new records read by `Auto Loader` to the
# end of the table,  without modifying or overwriting any existing data.
@dp.append_flow(target = tx_table_name, name = tx_flow_name)
def bronze_tx_flow():
    """
    Reads the inspection stream using `Auto Loader` (`cloudFiles`).

    It acts as a continuous sensor to ingest new `.json` files as they
    arrive.
    """
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .option("cloudFiles.inferColumnTypes", "true")  # Automatically infer schema
             .load(str(path_events_tx))
             .withColumn("ingestion_timestamp", current_timestamp())
             .withColumn("source_file", col("_metadata.file_path"))
    )

# ---------------- Events ingestion: Fraud labels (streaming) ----------------

labels_table_name = "bronze_labels"
labels_comment = """
This managed **`bronze` layer** table captures the delayed feedback (labels)
regarding the defectiveness of a product. It is asynchronously fed by a
streaming pipeline (`Auto Loader`) as the defectiveness of a product is
confirmed. It maintains the original raw format and includes lineage metadata,
setting the stage for an exact `JOIN` with `bronze_transactions` in downstream
layers.
"""

labels_flow_name = "bronze_labels_ingest_flow"

path_events_labels = vol_landing_zone / "events" / "labels"

dp.create_streaming_table(name = labels_table_name, comment = labels_comment)


@dp.append_flow(target = labels_table_name, name = labels_flow_name)
def bronze_labels_flow():
    """
    Reads the delayed defectiveness labels stream using `Auto Loader` (`cloudFiles`).

    Continuously ingests new feedback `.json` files from the landing zone.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(str(path_events_labels))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )