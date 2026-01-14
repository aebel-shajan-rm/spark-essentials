package part3typesdataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App{

  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json") /// most movies have at least one rating

  // select the first non-null value
  moviesDf.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)// if not null set to imdb rating
   ).show
  /*
+--------------------+----------------------+-----------+----------------------------------------------------+
|               Title|Rotten_Tomatoes_Rating|IMDB_Rating|coalesce(Rotten_Tomatoes_Rating, (IMDB_Rating * 10))|
+--------------------+----------------------+-----------+----------------------------------------------------+
|      The Land Girls|                  null|        6.1|                                                61.0|
|First Love, Last ...|                  null|        6.9|                                                69.0|
|I Married a Stran...|                  null|        6.8|                                                68.0|
|Let's Talk About Sex|                    13|       null|                                                13.0|
|                Slam|                    62|        3.4|                                                62.0|
| Mississippi Mermaid|                  null|       null|                                                null|
*/

  // checking for nulls
  moviesDf.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  // nulls when ordering
  moviesDf.orderBy(col("IMDB_Rating").desc_nulls_last)

  // removing nulls
  moviesDf.select("Title", "IMDB_Rating").na.drop()

  // replace nulls
  moviesDf.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
  moviesDf.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  ))

  // complex operations
  moviesDf.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating* 10) as nullif", /// returns null if the 2 values are Equal, else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if (first != null) second else third
  ).show


}

