package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /**
   * Reading a df:
   *  - format
   *  - schema or inferSchema = true
   *  - zero or more options
   */
  val carsDf = spark.read
    .format("json")
    .schema(carsSchema)
//    .option("mode", "failFast") // dropMalformed, permissive (default) // throws an exception for spark if a malformed record is encountered.
    .option("path", "src/main/resources/data/cars.json")
    .load()
//  carsDf.show() // lazy until action is called
  // org.apache.spark.SparkException: Malformed records are detected in record parsing. Parse Mode: FAILFAST. To process malformed records as null result, try setting the option 'mode' as 'PERMISSIVE'.

  // Alternative reading with options map
  val carsDfWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /**
   * Writing DFs
   *  - format
   *  - save mode = overwrite, append, ignore, errorIfExists
   *  - path
   *  - zero or more options
    */
  carsDf.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")


  // JSON flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // couple with schema; if spark fails parsing it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true") // CSV specific
    .option("sep", ",")
    .option("nullValue", "") // no notion of nulls in csv, so empty strings get converted to null
    .csv("src/main/resources/data/stocks.csv")

  // Parquet: open source compressed binary format. columnar
  carsDf.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet") // or .parquet

  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // Reading from a remote db
  val employeesDf = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDf.show()

  /**
   * Exercise: read movies data frame (movies.json). Write it to as:
   * - tab-seperated values file
   * - snappy parquet
   * - table public.movies in the Postgres DB */
  // Read in df
  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // TSV
  moviesDf.write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("sep", "\t")
    .csv("src/main/resources/data/exercises/movies.csv")

  // Parquet
  moviesDf.write
    .mode(SaveMode.Overwrite)
    .option("compression", "snappy") // not needed default
    .save("src/main/resources/data/exercises/movies.parquet")

  // psql db
  moviesDf.write
    .format("jdbc")
    .mode("overwrite")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()

}
