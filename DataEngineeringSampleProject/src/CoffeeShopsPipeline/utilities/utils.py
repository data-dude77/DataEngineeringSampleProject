

from pyspark.errors import PySparkException

# @udf(returnType=FloatType())
# def distance_km(distance_miles):
#     """Convert distance from miles to kilometers (1 mile = 1.60934 km)."""
#     return distance_miles * 1.60934



def does_table_exist(spark_session,table_name):
    try:
        spark_session.sql(
            f"SELECT 1 FROM {table_name} LIMIT 1"
        )
        return True
    except PySparkException as ex:
        if ex.getErrorClass() == "TABLE_OR_VIEW_NOT_FOUND":
            return False
        else:
            raise
