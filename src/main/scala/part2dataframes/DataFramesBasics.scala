package part2dataframes

import common.SparkCommon
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

object DataFramesBasics extends App with SparkCommon {

  val spark = simpleSparkSession("DataFrames Basics")

  // Read data frames
  val firstDF: DataFrame = readDf(spark, "cars.json")

  def firstLook(): Unit = {
    firstDF.show()
    firstDF.printSchema()
    firstDF.take(10).foreach(println)
  }

  // Types
  val longType = LongType

  // Schema of 'Cars' table
  val carSchema = sampleSchema

  // Get schema of a data frame
//  println(firstDF.schema)

  // In production, do not use "inferSchema" since Spark can mess things up
  // So, read a DF with our own schema

  val carsWithDFSchema = readDf(spark, "cars.json", Some(carSchema))

  // Create rows by hand
  val aCarRow =
    Row("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA")

  // Create DF from tuples
  // Using implicits
  import spark.implicits._
  val newCarTuples = List(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA")
  )

  val manualCarsDF = spark.createDataFrame(newCarTuples) // Schema is inferred by compiler based on tuple types

  // NOTE: DFs have schemas, Rows do not
  val dfUsingJsonReader = spark.read.schema(carSchema).json(dataPath("cars.json"))

//  dfUsingJsonReader.show() // Works just fine!

  val dfWithImplicits = newCarTuples.toDF()

//  dfWithImplicits.printSchema()
//  newCarTuples.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "Origin").printSchema()

  /**
    * Exercises
    * 1. Create a manual DF describing smartphones
    *  - Make, model, screen dimension, camera megapixels
    * 2. Read another file from data folder
    *  - print schema
    *  - count number of rows
    */

  // 1. Smartphones
  val smartphones = List(
    ("iPhone", "15", "6.2", "48"),
    ("Google", "Pixel 7", "6.7", "50"),
    ("Sony", "Xperia Z", "5.4", "12")
  )

  val smartphonesDF =
    smartphones.toDF("Make", "Model", "Screen Dimension (inches)", "Camera Megapixels")
  smartphonesDF.show()

  // 2. Read guitars
  val guitarSchema = StructType(
    Array(
      StructField("id", LongType, nullable = false),
      StructField("model", StringType, nullable = false),
      StructField("make", StringType, nullable = false),
      StructField("type", StringType, nullable = false)
    )
  )

  val guitarsDF = readDf(spark, "guitars.json", Some(guitarSchema))
  guitarsDF.printSchema()
  println(s"Number of rows in guitars data frame: ${guitarsDF.count()}")
}
