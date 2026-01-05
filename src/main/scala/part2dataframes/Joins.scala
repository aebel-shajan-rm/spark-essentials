package part2dataframes

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins extends App {
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")
  guitarsDf.printSchema()
  /*
root
 |-- id: long (nullable = true)
 |-- make: string (nullable = true)
 |-- model: string (nullable = true)
 |-- type: string (nullable = true)
   */

  val guitaritsDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")
  guitaritsDf.printSchema()

  /*
root
 |-- band: long (nullable = true)
 |-- guitars: array (nullable = true)
 |    |-- element: long (containsNull = true)
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)
   */

  val bandsDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")
  bandsDf.printSchema()

  /*
root
 |-- hometown: string (nullable = true)
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)
 |-- year: long (nullable = true)
   */

  // each guitarists have band id associated to it
  // inner joins
  val joinCondition = guitaritsDf.col("band") === bandsDf.col("id")
  val guitaristsBandsDf = guitaritsDf
    .join(bandsDf, joinCondition, "inner")
  guitaristsBandsDf.show()

  /*
+----+-------+---+------------+-----------+---+------------+----+
|band|guitars| id|        name|   hometown| id|        name|year|
+----+-------+---+------------+-----------+---+------------+----+
|   1|    [1]|  1| Angus Young|     Sydney|  1|       AC/DC|1973|
|   0|    [0]|  0|  Jimmy Page|     London|  0|Led Zeppelin|1968|
|   3|    [3]|  3|Kirk Hammett|Los Angeles|  3|   Metallica|1981|
+----+-------+---+------------+-----------+---+------------+----+
   */


  // outer joins
  // left outer = everything in the inner join + all the rows in the LEFT table, with nulls where the data is missing
  guitaritsDf.join(bandsDf, joinCondition, "left_outer").show()

  /*
+----+-------+---+------------+-----------+----+------------+----+
|band|guitars| id|        name|   hometown|  id|        name|year|
+----+-------+---+------------+-----------+----+------------+----+
|   0|    [0]|  0|  Jimmy Page|     London|   0|Led Zeppelin|1968|
|   1|    [1]|  1| Angus Young|     Sydney|   1|       AC/DC|1973|
|   2| [1, 5]|  2|Eric Clapton|       null|null|        null|null| // guitarist has null band
|   3|    [3]|  3|Kirk Hammett|Los Angeles|   3|   Metallica|1981|
+----+-------+---+------------+-----------+----+------------+----+

   */

  // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls where the data is missing
  guitaritsDf.join(bandsDf, joinCondition, "right_outer").show()

  /*
  +----+-------+----+------------+-----------+---+------------+----+
|band|guitars|  id|        name|   hometown| id|        name|year|
+----+-------+----+------------+-----------+---+------------+----+
|   1|    [1]|   1| Angus Young|     Sydney|  1|       AC/DC|1973|
|   0|    [0]|   0|  Jimmy Page|     London|  0|Led Zeppelin|1968|
|   3|    [3]|   3|Kirk Hammett|Los Angeles|  3|   Metallica|1981|
|null|   null|null|        null|  Liverpool|  4| The Beatles|1960| // beatles with no guitaris
+----+-------+----+------------+-----------+---+------------+----+
   */

  // full outer join = everything in the inner join + all the rows in the both tables, with nulls where the data is missing
  guitaritsDf.join(bandsDf, joinCondition, "outer").show()

  /*
+----+-------+----+------------+-----------+----+------------+----+ // five rows
|band|guitars|  id|        name|   hometown|  id|        name|year|
+----+-------+----+------------+-----------+----+------------+----+
|   0|    [0]|   0|  Jimmy Page|     London|   0|Led Zeppelin|1968|
|   1|    [1]|   1| Angus Young|     Sydney|   1|       AC/DC|1973|
|   3|    [3]|   3|Kirk Hammett|Los Angeles|   3|   Metallica|1981|
|   2| [1, 5]|   2|Eric Clapton|       null|null|        null|null| // null band
|null|   null|null|        null|  Liverpool|   4| The Beatles|1960| // null guitarist
+----+-------+----+------------+-----------+----+------------+----+
   */

  // semi-joins
  guitaritsDf.join(bandsDf, joinCondition, "left_semi").show()

  /*

+----+-------+---+------------+ // contains data from 1st data frame which satisfies joinCondition
|band|guitars| id|        name|
+----+-------+---+------------+
|   0|    [0]|  0|  Jimmy Page|
|   1|    [1]|  1| Angus Young|
|   3|    [3]|  3|Kirk Hammett|
+----+-------+---+------------+
   */

  // anti-joins
  guitaritsDf.join(bandsDf, joinCondition, "left_anti").show()

  /*
+----+-------+---+------------+ // contains data from 1st data frame which doesn't satisfy joinCondition
|band|guitars| id|        name|
+----+-------+---+------------+
|   2| [1, 5]|  2|Eric Clapton|
+----+-------+---+------------+
   */

  // things to bear in mind
  // both bands and guitarists have column id
  // guitaristsBandsDf.select("id", "band").show() // crashes
  /*
26/01/05 13:54:47 INFO DAGScheduler: Job 17 finished: show at Joins.scala:128, took 0.005056 s
Exception in thread "main" org.apache.spark.sql.AnalysisException: Reference 'id' is ambiguous, could be: id, id.;
   */
  //option 1 - rename column
  guitaritsDf.join(bandsDf.withColumnRenamed("id", "band"), "band").show() // spark knows to join on band when its just "band"

  // option 2 - drop the dupe column
  guitaristsBandsDf.drop(bandsDf.col("id")) // can't use just col(). spark maintains unique identifiers for all the columns it operates on

  // option 3 - rename the offending column and keep the data
  val bandsModDf = bandsDf.withColumnRenamed("id", "bandId")
  guitaritsDf.join(bandsModDf, guitaritsDf.col("band") === bandsModDf.col("bandId"))

  // using complex types.
  guitaritsDf.join(guitarsDf.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))


  /**
   * Exercises
   *
   * - Show all employees and their max salaries
   * - Show all employees who were never managers
   * - Find the job titles of the best paid 10 employees
   */
  def readTable(tableName: String): sql.DataFrame = {
    val DRIVER = "org.postgresql.Driver"
    val URL = "jdbc:postgresql://localhost:5432/rtjvm"
    val USER = "docker"
    val PASSWORD = "docker"
    spark.read
      .format("jdbc")
      .option("driver", DRIVER)
      .option("url", URL)
      .option("user", USER)
      .option("password", PASSWORD)
      .option("dbtable", s"public.$tableName")
      .load()
  }
  val employeesDf = readTable("employees")
  val salariesDf = readTable("salaries")
  val deptManagerDf = readTable("dept_manager")
  val titleDf = readTable("titles")

  // Exercise 1
  val maxSalariesDf = employeesDf
    .join(salariesDf, "emp_no")
    .groupBy("emp_no", "first_name")
    .agg(max("salary").as("max_salary"))
    .orderBy(col("max_salary").desc_nulls_last)
  maxSalariesDf.show()
  println("^ all employees and max salaries")

  // Exercise 2
  employeesDf
    .join(deptManagerDf, deptManagerDf.col("emp_no") === employeesDf.col("emp_no"), "left_anti")
    .show()
  println("^ all employees who were never managers")

  // Exercise 3 (got stuck)

  val latestTitles = titleDf
    .groupBy("emp_no", "title") // groups by emp_no and title
    .agg(max(col("to_date")).as("to_date"))
  val bestPaidEmployeesDf = maxSalariesDf
    .orderBy(col("max_salary").desc_nulls_last)
    .limit(10)
  val bestPaidJobsDf = bestPaidEmployeesDf.
    join(latestTitles, "emp_no")
    .orderBy(col("max_salary").desc)
  bestPaidJobsDf.show()
  println("^ Job titles of best paid 10 employees")
}

