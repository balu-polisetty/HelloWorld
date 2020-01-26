import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
case class moviesdf(id:Int,name:String,genre:String)
object Movies {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("xyz").master("local[*]").getOrCreate()
    import spark.implicits._
    val df1 = spark.read.option("inferSchema","true").option("delimiter",",")
      .csv(path = "C:\\Users\\Harpreet\\IdeaProjects\\HelloWorld\\src\\main\\scala\\MoviesFile.dat").toDF("id","name","genre")



    df1.show()
    df1.createOrReplaceTempView("moviesview")
    import spark.implicits._
    val newdf = df1.withColumn("tmp",Split($"genre","|")
      .select($"id",$"name",$"tmp".getItem(0).as("firstCol"),
      $"tmp".getItem(1).as("SecondCol"),
      $"tmp".getItem(2).as("ThirdCol"),
      $"tmp".getItem(3).as("FourthCol"),
      $"tmp".getItem(4).as("fifthCol")
    )).drop("tmp")

    newdf.show()
    //spark.sql("select * from moviesview where genre like '%Adven%'").show()

    //df1.write.csv("C:\\Users\\Harpreet\\IdeaProjects\\HelloWorld\\src\\main\\scala\\Moviescsvfile")

  }
}
