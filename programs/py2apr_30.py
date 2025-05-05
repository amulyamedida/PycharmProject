from pyspark.sql import *
from  pyspark.sql.functions import *
spark=SparkSession.builder.appName("test").master("local").getOrCreate()
data=r"E:\BigData\drivers\bank-full.csv"
df=spark.read.format("csv").option("inferschema","True").option("sep",";").option("header","true").load(data)
#res=df.groupBy("marital").agg(count("*").alias("cnt")).orderBy(col("cnt"),ascending=True)
#res.show()

#In sql we use select like that here also
#res=df.select("marital","job","balance").where(col("job")=="management")
#res.show()

#If we want multiple condition use like this df.where(() & ())
#res=df.where((col("job")=="management")&(col("loan")=="yes"))
#res.show()

#If we want to print particular data except one then use not operator like this ~
#res=df.where(~(col("marital")=="married"))
#res.show()

#In sql we use between, in place of where we can use filter also
#res=df.where(col("age").between(50,60))
#res.show()

#In sql we use like operator
#res=df.where(col("job").rlike("r%d"))
#res.show()

#withcolumn is used to add a new column if already exists then it updates the column
res=df.withColumn("grade",when(col("age")>60, "oldage")
                  .when(col("age").between(30,60),"gentleman")
                  .when(col("age")<30,"minor"))
res.show()