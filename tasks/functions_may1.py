from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("test").master("local").getOrCreate()

data=""
df=spark.read.format("csv").option("inferschema","True").option("sep",",").option("header","true").load(data)