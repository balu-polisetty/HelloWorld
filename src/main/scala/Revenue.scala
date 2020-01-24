import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Revenue {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("Revenues for each day").master("local[*]").getOrCreate()

    val Ordersdf = spark.read.option("inferSchema","true").option("header","true")
      .csv("C:\\Users\\Harpreet\\Desktop\\Python Cheatsheet\\SRUJANA SPARK\\orders.csv")

    Ordersdf.show()

    val Orderitemsdf = spark.read.option("inferSchema","true").option("header","true")
      .csv("C:\\Users\\Harpreet\\Desktop\\Python Cheatsheet\\SRUJANA SPARK\\orderitems.csv")
    Orderitemsdf.show()

  }

}
