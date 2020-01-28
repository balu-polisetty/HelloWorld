import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.expressions.MutableAggregationBuffer

import scala.util.matching.Regex

object LogData {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Logdata").master("local[*]").getOrCreate()

    val logdf = spark.read.option("header", "false")

      .csv("C:\\Users\\Harpreet\\IdeaProjects\\HelloWorld\\src\\main\\scala\\bigdatamatica")

   // logdf.show(5)
    import spark.implicits._
    val logrdd = logdf.rdd
    //SAMPLE REGEX , IMPORT SCALA.UTIL.MATCHING.REGEX,EX=> FOR SINGLE CHARACTER MATCH[#]
    val a  = "[^a-zA-z0-9]".r()   //.r is the method to create regex

    // REPLACING "-" WITH BLANK IN THE RDD USING REGEX
    val newrdd = logrdd.map(x => x.toString())
                .map(x => x.replaceAll("[-]",""))

    newrdd.take(5).foreach(println)

    /*val cleandf = newrdd.toDF("ipAddress","Timestamp","method","path","no","version","os","device","dversion","buid",
    "appkit","khtml","browser","compat","site","abc","xyz","1","2")
    cleandf.show(5)*/
  }
}