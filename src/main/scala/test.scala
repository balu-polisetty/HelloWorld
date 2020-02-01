import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").appName("test").getOrCreate()
    val file = spark.sparkContext.textFile("/home/shankaar/Datasets/bigdatamatica.com-Oct-2019")
    var mapout = file.map(x=>x.split(" "))
    mapout.collect().foreach(println)
    mapout.map(x=>x.map(x => {println(x)})).collect().take(1)
  }

}
