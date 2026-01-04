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
    StructField("Year", StringType),
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

}
