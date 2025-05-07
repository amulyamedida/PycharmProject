from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import dense_rank

spark = SparkSession.builder.appName("test").master("local").getOrCreate()
data=r"E:\BigData\drivers\us-500.csv"
df=spark.read.format("csv").option("inferSchema","true").option("mode","DROPMALFORMED").option("header","true").option("sep",",").load(data)
df=df.withColumnRenamed("zip","salary") #to rename column name
#res=df.orderBy(col("salary").asc())
win=Window.partitionBy("state").orderBy(col("salary").asc())
res=(df.withColumn("rank",rank().over(win))
     .withColumn("drank",dense_rank().over(win))
     .withColumn("rno",row_number().over(win))
    .withColumn("ntile",ntile(4).over(win)).where(col("state")=="CA") #it calculates total/ntile count,based on the result upto that number shows rank 1 like that

     )
res.show()

#buck = floor(row_number() -1 *n /total_rows) +1