package common

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
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

  def readDf(
    sparkSession: SparkSession,
    file: String,
    schema: Option[StructType] = None,
    format: Option[String] = None,
    options: Map[String, String] = Map.empty
  ): DataFrame = {
    val reader = sparkSession.read
      .format(format.getOrElse(file.split('.').last))
      .schema(schema.orNull) // Automatically infer schema if None
      .options(options)

    format match {
      case Some("jdbc") => reader.load()
      case _            => reader.load(dataPath(file))
    }
  }

  def writeDf(
    df: DataFrame,
    sparkSession: SparkSession,
    file: String,
    format: String = "json",
    mode: SaveMode = SaveMode.Overwrite,
    options: Map[String, String] = Map.empty
  ): Unit = {
    val writer = df.write
      .format(format)
      .mode(mode)
      .options(options)

    if (format == "jdbc") writer.save() else writer.option("path", dataPath(file)).save()
  }

  def dataPath(file: String): String = s"src/main/resources/data/$file"
}
