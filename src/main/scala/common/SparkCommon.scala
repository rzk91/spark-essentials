package common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

trait SparkCommon {

  def simpleSparkSession(name: String): SparkSession =
    SparkSession
      .builder()
      .appName(name)
      .config("spark.master", "local")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()

  def sampleSchema: StructType =
    StructType(
      Array(
        StructField("Name", StringType),
        StructField("Miles_per_Gallon", DoubleType),
        StructField("Cylinders", LongType),
        StructField("Displacement", DoubleType),
        StructField("Horsepower", LongType),
        StructField("Weight_in_lbs", LongType),
        StructField("Acceleration", DoubleType),
        StructField("Year", StringType),
        StructField("Origin", StringType)
      )
    )

  def dataPath(table: String, ending: String = "json"): String = s"src/main/resources/data/$table.$ending"
}
