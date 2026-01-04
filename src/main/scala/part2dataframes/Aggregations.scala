package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {
  val spark = SparkSession.builder()
    .appName("Aggregation and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // Counting
  val genresCountDf = moviesDf.select(count(col("Major_Genre"))) // all the values except null
  moviesDf.selectExpr("count(Major_Genre)") // equivalent

  // counting all rows
  moviesDf.select(count("*")) // counts all rows and includes nulls

  // counting distinct
  moviesDf.select(countDistinct(col("Major_Genre"))).show()

  // approximate counts
  moviesDf.select(approx_count_distinct(col("Major_Genre"))).show() // doesn't scan db row by row

  // min and max
  val minRatingDf = moviesDf.select(min(col("IMDB_Rating")))
  moviesDf.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDf.select(sum(col("US_Gross")))
  moviesDf.selectExpr("sum(US_Gross)")
  moviesDf.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDf.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // Data science
  moviesDf.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  // Grouping
  val countByGenreDf = moviesDf
    .groupBy(col("Major_Genre")) // also includes null
    .count() // select count(*) from moviesDF group by Major_Genre

  val avgRatingByGenreDf = moviesDf
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationsByGenreDb = moviesDf
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  aggregationsByGenreDb.show()

  /**
   * Exercises
   *
   * 1. Sum up All the profits of all the movies in the df
   * 2. count how many distinct directors we have
   * 3. show the mean and standard deviation of US gross revenue for the movies
   * 4. compute the average IMDB rating and the average us gross revenue per director
   */

  // Exercise 1
  val summedProfits = moviesDf.select(sum("Worldwide_Gross"))
  summedProfits.show()

  // Exercise 2
  val distinctDirectors = moviesDf.select(countDistinct("Director"))
  distinctDirectors.show()

  // Exercise 3
  val meanAndDev = moviesDf.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
  )
  meanAndDev.show()

  // Exercise 4
  val aggPerDirector = moviesDf
    .groupBy(col("Director"))
    .agg(
      avg(col("IMDB_Rating")).as("average_rating"),
      sum(col("US_Gross"))
    )
    .orderBy(col("average_rating").desc_nulls_last)
  aggPerDirector.show()
}
