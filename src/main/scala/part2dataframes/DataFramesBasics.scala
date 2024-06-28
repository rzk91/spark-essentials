package part2dataframes

import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DataFramesBasics extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // Read data frames
  val firstDF: DataFrame = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  def firstLook(): Unit = {
    firstDF.show()
    firstDF.printSchema()
    firstDF.take(10).foreach(println)
  }

  // Types
  val longType = LongType

  // Schema of 'Cars' table
  val carSchema = StructType(Array(
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

  // Get schema of a data frame
//  println(firstDF.schema)

  // In production, do not use "inferSchema" since Spark can mess things up
  // So, read a DF with our own schema

  val carsWithDFSchema = spark.read
    .format("json")
    .schema(carSchema)
    .load("src/main/resources/data/cars.json")

  // Create rows by hand
  val aCarRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  // Create DF from tuples
  val newCarTuples = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(newCarTuples) // Schema is inferred by compiler based on tuple types

  // NOTE: DFs have schemas, Rows do not
  val dfUsingJsonReader = spark.read.schema(carSchema).json("src/main/resources/data/cars.json")

//  dfUsingJsonReader.show() // Works just fine!

  // Using implicits
  import spark.implicits._
  val dfWithImplicits = newCarTuples.toDF()

  dfWithImplicits.printSchema()
  newCarTuples.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "Origin").printSchema()
}
