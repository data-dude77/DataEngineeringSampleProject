import uuid
import pyspark.sql.functions as F
from pyspark.sql.types import StringType



def get_new_column(cols, keyword):
    res=[f"{keyword}_{col}" for col in cols]
    new_col_dict= dict(zip(cols,res))
    return new_col_dict

@F.udf(returnType=StringType())
def create_unique_uuid():
    return str(uuid.uuid4())
