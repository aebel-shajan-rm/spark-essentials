package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {
  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDf.show()

  // Columns
  val firstColumns = carsDf.col("Name")

  // selecting (projecting)
  val carNamesDf = carsDf.select(firstColumns) // obtain new df from original df

  // Various select methods
  import spark.implicits._
  carsDf.select(
    carsDf.col("Name"), // similar to just col
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower", // Fancier interpolated string, returns a column object
    expr("Origin"), // Expression, returns just the origin column
  )

  // select with plain column names
  carsDf.select("Name", "Year") // another df

  // EXPRESSIONS
  val simplestExpression = carsDf.col("Weight_in_lbs")
  val weightInKgExpression = carsDf.col("Weight_in_lbs") / 2.2 // column object returned describes transformation

  val carsWithWeightsDf = carsDf.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_from_expr")
  )

  // selectExpr
  val carsWithSelectExprWeightsDf = carsDf.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing

  // adding a column
  val carsWithKg3Df = carsDf.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // renaming a column
  val carsWithColumnRenamed = carsDf.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds") //
  // careful with column names
//  carsWithColumnRenamed.selectExpr("`Weight in pounds`") // gives error
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // Filtering
  val europeanCarsDf = carsDf.filter(col("Origin") =!= "USA")
  val europeanCarsDf2 = carsDf.where(col("Origin") =!= "USA")
  // filtering with expression strings
  val americanCarsDf = carsDf.filter("Origin = 'USA'")
  // chain filters
  val americanPowerfulCarsDf = carsDf.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDfChainedWithAnd= carsDf.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDfChainedWithString = carsDf.filter("Origin = 'USA' and Horsepower > 150")

  // Unioning more rows
  val moreCarsDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDf = carsDf.union(moreCarsDf) // works if the dfs have the same schema

  // distinct
  val allCountriesDF = carsDf.select("Origin").distinct()
  allCountriesDF.show()

}
