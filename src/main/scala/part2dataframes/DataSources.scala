package part2dataframes

import common.SparkCommon
import common.implicits.{enrichMaps, enrichSparkSchema}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._

object DataSources extends App with SparkCommon {

  val spark = simpleSparkSession("Data Sources and Formats")
  val carSchema = sampleSchema

  /*
    Reading a DF requires:
    - format
    - schema (optional if inferSchema = false; by default "inferSchema" is true unless schema is provided)
    - zero or more options
      - mode -> what to do if data is faulty (can be dropMalformed, failFast, or permissive (default))
      - path -> provide the path where the data is located (call just .load() afterwards)
      - ...
   */
  val carsDf = spark.read
    .format("json")
    .schema(carSchema)
    .option("mode", "failFast")
    .load(dataPath("cars"))

  val carsDfWithOptionMap = spark.read
    .format("json")
    .options(
      Map(
        "mode"        -> "failFast",
        "path"        -> dataPath("cars"),
        "inferSchema" -> "true"
      )
    )
    .load()

//  carsDf.show()

  /*
    Writing data frames requires:
    - format
    - save mode -> overwrite, append, ignore, errorIfExists
    - path
    - zero or more options (bucketing, partitioning, etc.)
   */
  carsDf.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", dataPath("cars_duplicate"))
//    .save()

  // Formats
  val newCarSchema = carSchema.changeType("Year", DateType)

  // JSON flags
  spark.read
    .schema(newCarSchema)
    .option("dateFormat", "YYYY-MM-dd")    // If Spark fails while parsing, it will put null
    .option("allowSingleQuotes", "true")   // Useful if JSONs use ' instead of "
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate, uncompressed
    .json(dataPath("cars"))

  // CSV flags
  val stocksSchema = StructType(
    Array(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    )
  )

  spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd yyyy")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv(dataPath("stocks", "csv"))
//    .show()

  // Parquet flags
  carsDf.write
    .mode(SaveMode.Overwrite)
//    .save(dataPath("cars", "parquet")) // Automatically inferred as parquet

  // Text files
  spark.read
    .text(dataPath("sample_text", "txt"))
//    .show()

  // Reading from a remote database (e.g. PostgreSQL)
  val driver = "org.postgresql.Driver"
  val dbUrl = "jdbc:postgresql://localhost:25432/rtjvm"
  val username = "docker"
  val password = "docker"
  val properties = Map(
    "driver"   -> driver,
    "user"     -> username,
    "password" -> password
  ).asProperties

  val employeesDf = {
    val oneOption = spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", dbUrl)
      .option("user", username)
      .option("password", password)
      .option("dbtable", "public.employees")
      .load()

    val betterOption = spark.read
      .jdbc(
        url = dbUrl,
        table = "public.employees",
        properties = properties
      )

    betterOption
  }

//  employeesDf.show()

  /**
    * Exercise: Read the movies DF and then write it as
    *  - tab-separated values files
    *  - snappy parquet
    *  - table "public.movies" in the Postgres DB
    */
  val moviesDf = spark.read.json(dataPath("movies"))

  // 1. TSVs
  moviesDf.write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("sep", "\t")
//    .option("nullValue", "") // default
    .csv(dataPath("movies", "csv"))

  // 2. Snappy parquet
  moviesDf.write
    .mode(SaveMode.Overwrite)
//    .option("compression", "snappy") // Default is snappy
    .save(dataPath("movies", "parquet"))

  // 3. Postgres DB table
  moviesDf.write
    .mode(SaveMode.Overwrite)
    .jdbc(
      url = dbUrl,
      table = "public.movies",
      connectionProperties = properties
    )
}
