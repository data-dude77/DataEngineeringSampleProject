from pyspark.sql.functions import udf
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType


# @udf(returnType=FloatType())
# def distance_km(distance_miles):
#     """Convert distance from miles to kilometers (1 mile = 1.60934 km)."""
#     return distance_miles * 1.60934

@udf(returnType=FloatType())
def calculate(df, latitude1,longitude1,latitude2,longitude2):
        km_to_miles_factor=0.621371
        df=df.withColumn("DistanceMiles", 
            (F.round((F.acos((F.sin(F.radians(F.col(latitude1))) * F.sin(F.radians(F.col(latitude2)))) + \
                   ((F.cos(F.radians(F.col(latitude1))) * F.cos(F.radians(F.col(latitude2)))) * \
                    (F.cos(F.radians(longitude1) - F.radians(longitude2))))
                       ) * F.lit(6371.0)), 4))*F.lit(km_to_miles_factor))
        return df
