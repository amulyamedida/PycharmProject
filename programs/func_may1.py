from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("test").master("local").getOrCreate()

data=r"E:\BigData\drivers\donations.csv"
allconfig={
    "header":"true",
    "inferschema":"true",
    "mode":"DROPMALFORMED"
}
df=spark.read.format("csv").options(**allconfig).load(data) #here ** are used for dictionary type for array use single star(*)
df=df.withColumn("dt",to_date(col("dt"),"d-m-yyyy"))
df.show()
#In spark by default date considered as yyyy-MM-dd
