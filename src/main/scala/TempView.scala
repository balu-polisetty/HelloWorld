import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}

case class person(name:String, age:Int)

object TempView {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("tempview").master("local[*]").getOrCreate()

    import spark.implicits._

    val df = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\Harpreet\\IdeaProjects\\HelloWorld\\src\\main\\Input Files\\people")


    df.show()

    /*val ds = df.as[person]
    ds.printSchema()
    ds.show()*/

  }

}
