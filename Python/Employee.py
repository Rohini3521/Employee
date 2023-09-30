from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .master("local[3]") \
    .appName("Employee") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

raw = spark.read.json("D:/EmployeeChallenge/input.json")


colRen = raw.withColumnRenamed("Permanent address", "Permanent_address").withColumnRenamed("current Address", "current_address")


grpcurr = colRen.groupBy("current_address").count()


age30 = colRen.filter(col("Age") > 30).select("Name")

colRen.createOrReplaceTempView("temp")

tmp = spark.sql("select * from temp")

colRen.createGlobalTempView("glob")

glob = spark.sql("select * from global_temp.glob").show()
