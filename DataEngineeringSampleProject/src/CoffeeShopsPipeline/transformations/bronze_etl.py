from pyspark import pipelines as dp
from utilities import utils


table_exists=utils.does_table_exist(spark,"business.coffee_shops.raw_data_table")

@dp.table(
    name="business.coffee_shops.raw_data_streaming_table",
    comment="Creating a raw STREAMING table of suppliers,customers,franchises and sales transations",
)
def create_streaming_raw_table():
    if table_exists:
      return spark.readStream.table("business.coffee_shops.raw_data_table")
    else:
        return spark.readStream.table("business.coffee_shops.raw_data_mv")
