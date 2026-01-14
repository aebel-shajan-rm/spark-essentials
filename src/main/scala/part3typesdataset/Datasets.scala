package part3typesdataset

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double],
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: String,
                Origin: String,
              )

object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()


  val numbersDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDf.printSchema()

  // convert a DF to a Dataset
  implicit val intEncoder = Encoders.scalaInt
  val numbersDs: Dataset[Int] = numbersDf.as[Int]

  // dataset of a complex type
  // 1 - define your case class


  // 2 - read the df from the file
  def readDf(fileName: String) = spark.read
    .option("inferSchema", "true").json(s"src/main/resources/data/$fileName")

  // 3 - define an encoder (importing the implicits)

  import spark.implicits._

  val carsDf = readDf("cars.json")
  // 4 - convert the Df to Ds
  private val carsDs = carsDf.as[Car]

  // DS collection functions
  numbersDs.filter(_ < 100).show

  // map, flatMap, fold, reduce, for comprehensions
  val carNamesDs = carsDs.map(car => car.Name.toUpperCase())

  carNamesDs.show

  /** *
   * Exercises
   *
   * 1. Count how many cars we have
   * 2. Count how many powerful cars we have (horsepower > 140)
   * 3. Compute Average HP for entire dataset.
   *
   */

  // 1. how many cars
  val count = carsDs.count()

  // 2.
  //  carsDs.printSchema()
  val filteredCount = carsDs.filter( car => car.Horsepower.getOrElse(0L) > 140).count()

  // 3
  val cumHp = carsDs.map(car => car.Horsepower.getOrElse(0L)).reduce((horsepower, total) => total + horsepower)
    println(count, filteredCount, cumHp/count)

  carsDs.select(avg("Horsepower")).show()
}


