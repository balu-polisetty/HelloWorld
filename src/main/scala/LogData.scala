import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object LogData {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Logdata").master("local[*]").getOrCreate()

    val logdf = spark.read.option("header", "false")

      .csv("C:\\Users\\Harpreet\\IdeaProjects\\HelloWorld\\src\\main\\scala\\bigdatamatica")

    logdf.show(5)

  }
}