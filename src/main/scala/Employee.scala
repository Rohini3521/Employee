import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object Employee {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("emp")
      .getOrCreate()

    val raw = spark.read.json("D:/Fresco_Employee/input.json")

    val colRen = raw.withColumnRenamed("Permanent address","Permanent_address")
      .withColumnRenamed("current Address","current_address")

    val grpcurr = colRen.groupBy("current_address").count()

    val age30 = colRen.filter(col("Age") > 30).select("Name")

    colRen.createOrReplaceTempView("temp")
    val tmp = spark.sql("select * from temp")

    colRen.createGlobalTempView("glob")
    val glob = spark.sql("select * from global_temp.glob").show()


  }

}
