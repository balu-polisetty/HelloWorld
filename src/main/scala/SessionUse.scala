import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SessionUse {
  def main(args: Array[String]): Unit = {
    //UNDERSTANDING CLOSURES AND ACCUMULATORS
    val conf = new SparkConf().setAppName("Session1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    /*var counter = 0 ;
    val data = Array(1,2,3,4,5)
    val parRdd = sc.parallelize(data)
    parRdd.foreach(x => counter += x)
    println("counter value ="+counter) */

    var accum = sc.longAccumulator("counter")
    val data = Array(1,2,3,4,5)
    val parRdd = sc.parallelize(data)
    parRdd.foreach(x => accum.add(x))
    println("Acccumulator value is "+accum)
  }

}
