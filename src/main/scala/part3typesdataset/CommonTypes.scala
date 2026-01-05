package part3typesdataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {
  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to a DF
  moviesDf.select(col("Title"), lit(47).as("plain_value")).show()

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_RATING") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDf.select("Title").where(dramaFilter)
  // + multiple ways of filtering
  val moviesWithGoodnessFlagsDf = moviesDf.select(col("Title"), preferredFilter.as("good_movie")) // boolean column from filter expression
  // filter on boolean column
  moviesWithGoodnessFlagsDf.where("good_movie") // where(col("good_movie") === "true")

  // negations
  moviesWithGoodnessFlagsDf.where(not(col("good_movie")))

  // Numbers
  // math operators
  val moviesAvgRatingsDf = moviesDf.select(col("Title"), col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating") / 2)

  // Correlation = number between -1 and 1
  println(moviesDf.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) // corr is an action

  // Strings
  val carsDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // initcap, lower, upper
  carsDf.select(initcap(col("Name"))).show() // capitalises  first letter of every word

  // contains
  carsDf.select("*").where(col("Name").contains("volkswagen")).show()

  // regex
  val regexString = "volkswagen|vw"
  val vwDf = carsDf.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract") // col, regexstring, index of matched strings
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  vwDf.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  ).show()

  /**
   * Exercise
   *
   * Filter the cars DF by a list of car names obtained by an API call
   * Versions:
   *  - contains
   *  - regexe
   */
  def getCarName: List[String] = List("Volkswage", "Mercedes-Benz", "Ford")

  val complexRegex = getCarName.map(_.toLowerCase()).mkString("|")
  carsDf
    .select(
      col("Name"),
      regexp_extract(col("Name"), complexRegex, 0).as("regex_extract") // col, regexstring, index of matched strings
    )
    .where(col("regex_extract") =!= "")
    .drop("regex_extract")
    .show()
}
