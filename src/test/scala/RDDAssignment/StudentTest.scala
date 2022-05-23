package RDDAssignment

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import utils.{Commit, Loader}

import scala.reflect.io.Path

/**
  * This class contains the necessary boilerplate code for testing with Spark. This contains the bare minimum to run
  * Spark, change as you like! It is highly advised to write your own tests to test your solution, as it can give you
  * more insight in your solution. You can also use the
  */
class StudentTest extends FunSuite with BeforeAndAfterAll {

//  This is mostly boilerplate code, read the docs if you are interested!
  val spark: SparkSession = SparkSession
    .builder
    .appName("Spark-Assignment")
    .master("local[*]")
    .getOrCreate()

  implicit val sql: SQLContext = spark.sqlContext

  import spark.implicits._

  val commitDF: DataFrame = Loader.loadJSON(Path("data/data_raw.json"))
  commitDF.cache()

  val commitRDD = commitDF.as[Commit].rdd
  commitRDD.cache()

  test("Example test for students") {
    RDDAssignment.assignment_6(commitRDD)
  }

  override def afterAll(): Unit = {
    //    Uncomment the line beneath if you want to inspect the Spark GUI in your browser, the url should be printed
    //    in the console during the start-up of the driver.
//        Thread.sleep(9999999)
    spark.close()
  }

}