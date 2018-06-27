from __future__ import print_function
import numpy as np
from pyspark.sql import SparkSession
import pandas as pd
#import statsmodels as sm


spark = SparkSession\
    .builder\
    .appName("PySpark @panda-udfs")\
    .getOrCreate()

spark.conf.get("spark.executor.instances")
spark.conf.get("spark.executor.cores")

df = spark.createDataFrame(
    [("a", 1, 0), ("a", -1, 42), ("b", 3, -1), ("b", 10, -2)],
    ("key", "value1", "value2")
)
df.cache()
df.count()
df.show()

from pyspark.sql.types import *
schema = StructType([
    StructField("key", StringType()),
    StructField("avg_min", DoubleType())
])
schema

from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import PandasUDFType

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def g(df):
    result = pd.DataFrame(df.groupby(df.key).apply(
        lambda x: x.loc[:, ["value1", "value2"]].min(axis=1).mean()
    ))
    result.reset_index(inplace=True, drop=False)
    return result
  
df.groupby("key").apply(g).show()