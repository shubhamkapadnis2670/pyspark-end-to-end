from pyspark.sql.functions import *
from pyspark.sql.types import *


@udf(returnType=IntegerType())
def column_split_count(column):
    return len(column.split(" "))


@udf(returnType=StringType())
def string_concat(col):
    pass
