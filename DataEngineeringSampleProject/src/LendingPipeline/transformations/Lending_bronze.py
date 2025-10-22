from pyspark import pipelines as dp
from pyspark.sql.functions import col
from utilities import utils


# Ingest Raw Data in Bronze Table
@dp.table(
    name="business.lending.bronze_streaming_table",
    comment="Create Raw Streaming Data Table. A bronze table",
)
def create_raw_lending_streaming_table():
    lending_club_dataset_path = "/databricks-datasets/samples/lending_club/parquet/"
    static_lending_df = spark.read.format("parquet").load(lending_club_dataset_path)
    schema = static_lending_df.schema
    return (
        spark.readStream.format("parquet")
        .schema(schema)
        .load(lending_club_dataset_path)
    )
