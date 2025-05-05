from pyspark.sql import *
from  pyspark.sql.functions import *
spark=SparkSession.builder.appName("test").master("local").getOrCreate()
data=r"E:\BigData\drivers\aadharpancarddata.csv"
df=spark.read.format("csv").option("header","true").load(data)
df.show(n=5,truncate=False)
df.printSchema()
