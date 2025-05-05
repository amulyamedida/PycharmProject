from pyspark.sql import *
from  pyspark.sql.functions import *
from pyspark.sql.functions import regexp_replace

spark=SparkSession.builder.appName("test").master("local").getOrCreate()
data=r"E:\BigData\drivers\us-500.csv"
df=spark.read.format("csv").option("inferschema","True").option("sep",",").option("header","true").load(data)
#res=df.withColumn("phone1",regexp_replace(col("phone1"),"-",""))
#If we want to remove multiple special characters the keep all those in [-+(/)]
res=(df.withColumn("phone1",regexp_replace(col("phone2"),"[-(]",""))
     .withColumn("zip",rpad(col("zip"),5,"0"))
     #if we want to keep some characters and except some of them we want to print remain then we can use ^
     #here except small and capital letters from a-z and A-Z print all
     #if the email contains braces like () [ { ] } then use "\" followed by this use ] etc
     .withColumn("email",regexp_replace(col("email"),"[^a-zA-Z0-9@]",""))
     #in sql we have concat like here we are combining first and last names ito another column name as full name, for space use lit(" ")
     .withColumn("full_name",concat(col("first_name"),lit(" "),col("last_name")))
     .withColumn("ful-name", concat_ws("_", col("first_name"), col("last_name")))
     .withColumn("first", (col("first_name") + col("last_name"))) #here + is not a function but it doesn't show error it displays null
     )
#res.show()

sol=df.groupBy("state").agg(count("first_name"),countDistinct("first_name").alias("counting"),collect_set(col("first_name")),collect_list(col("first_name")))
#by default truncate print first 20 characters and want to filter repeated names then use distinct as countDistinct
sol.show()