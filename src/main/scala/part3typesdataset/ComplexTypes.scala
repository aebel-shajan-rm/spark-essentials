package part3typesdataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {
  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY") // the exercise used legacy time parser policy


  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates
  val moviesWithReleaseDates = moviesDf
    .select(
      col("Title"),
      to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release") // conversion
    )

  moviesWithReleaseDates
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // date_add, date_sub

  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull).show()

  /**
   * Exercise
   * 1. How do we deal with multiple date formats?
   * 2. Read the stocks DF and parse the dates
   */

  //1 parse dataframe multiple times

  // 2
  val stocksDf = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/stocks.csv")

  val stocksDfWithDates = stocksDf
    .withColumn("actual_date", to_date(col("date"), "MMM dd yyyy"))

  stocksDfWithDates.show()

  // Structures : groups of columns aggregated into one

  // Version 1 with column operators
  moviesDf
    .select(
      col("Title"),
      struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")
    )
    .select(
      col("Title"),
      col("Profit").getField("US_Gross").as("US_Profit")
    )
    .show()

  // Version 2 with expression strings
  moviesDf
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays
  val moviesWithWords = moviesDf.select(col("Title"), split(col("Title"), " |,").as("Title_Words")) // Array of strings

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  ).show()
}
