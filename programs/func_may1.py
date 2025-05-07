from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("test").master("local").getOrCreate()

data=r"E:\BigData\drivers\donations.csv"
allconfig={
    "header":"true",
    "inferschema":"true",
    "mode":"DROPMALFORMED"
}
#when we want to use USA time based data then we need to include below line
spark.conf.set("spark.sql.session.timezone","CST")
df=spark.read.format("csv").options(**allconfig).load(data) #here ** are used for dictionary type for array use single star(*)
#In spark by default date considered as yyyy-MM-dd
#to_date function convert our input to spark undertstandable format (yyyy-MM-dd)
#datediff (old_date,new_date) it gives difference between two dates in int format
df=df.withColumn("dt",to_date(col("dt"),"d-m-yyyy"))
#if we want to do any operations initially we must convert to to_date otherwise it considered as string
#except datediff remaining all seperated with "_"
df=(df.withColumn("today",current_date())
    .withColumn("dtdiff", datediff(col("dt"), col("today")))
    .withColumn("dtdiffer", col("dt") - col("today"))
    .withColumn("ts", current_timestamp())
    .withColumn("dtadd", date_add(col("dt"), 100))
    .withColumn("dtadd",date_sub(col("dt"), -111))
    #date_format (dt,required_format) by default it is dd-MM-yyyy it is used to convert into our required format
    .withColumn("dateformat",date_format(col("dt"),"dd-MMM-yyyy-EEEE")) #if we want only days then its possible just give only "EEEE"
    .withColumn("lastdate",last_day(col("dt"))) #it returns last day of the month
    .withColumn("dayofyear",dayofyear(col("today"))) #it returns how many days till today in integer format
    .withColumn("dayofmonth",dayofmonth(col("today")))
    .withColumn("dayofweek",dayofweek(col("dt"))) #when we are checking with previous years it gives like 1,3,7 these indicates 1(sun),2(mon)....7(saturday)
    .withColumn("nextday",next_day(col("dt"),"Fri")) #next day works only applicable for 7 working days only
    .withColumn("datetrunc",date_trunc("day",col("ts"))) #whatever we specify from all will be delete
    .withColumn("monthsadd", add_months(col("dt"),20))
    .withColumn("mon",monotonically_increasing_id())
    .withColumn("lastfri", next_day(date_add(last_day(col("dt")), -7), "Fri"))
    )

df=df.withColumn(
    "last_working_day",
    expr("""
        CASE 
            WHEN dayofweek(last_day(dt)) = 1 THEN date_add(last_day(dt), -2)  -- Sunday, go back 2 days
            WHEN dayofweek(last_day(dt)) = 7 THEN date_add(last_day(dt), -1)  -- Saturday, go back 1 day
            ELSE last_day(dt)  -- Weekday
        END
    """)
)

df.show(truncate=False)
