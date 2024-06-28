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
  val carsDf = readDf(spark, "cars.json", Some(carSchema), options = Map("mode" -> "failFast"))

  val carsDfWithOptionMap = spark.read
    .format("json")
    .options(
      Map(
        "mode"        -> "failFast",
        "path"        -> dataPath("cars.json"),
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
    .option("path", dataPath("cars_duplicate.json"))

  writeDf(carsDf, spark, "cars_duplicate.json")

  // Formats
  val newCarSchema = carSchema.changeType("Year", DateType)

  // JSON flags
  readDf(
    spark,
    "cars.json",
    Some(newCarSchema),
    options = Map(
      "dateFormat"        -> "YYYY-MM-dd",  // If Spark fails while parsing, it will put null
      "allowSingleQuotes" -> "true",        // Useful if JSONs use ' instead of "
      "compression"       -> "uncompressed" // bzip2, gzip, lz4, snappy, deflate, uncompressed
    )
  )

  // CSV flags
  val stocksSchema = StructType(
    Array(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    )
  )

  readDf(
    spark,
    "stocks.csv",
    Some(stocksSchema),
    options = Map(
      "dateFormat" -> "MMM dd yyyy",
      "header"     -> "true",
      "sep"        -> ",",
      "nullValue"  -> ""
    )
  )
//    .show()

  // Parquet flags (default when .save is called without specifying a format)
  carsDf.write.mode(SaveMode.Overwrite)
//    .save(dataPath("cars", "parquet")) // Automatically inferred as parquet

  writeDf(
    carsDf,
    spark,
    "cars_parquet",
    "parquet"
  )

  // Text files
  readDf(
    spark,
    "sample_text.txt",
    format = Some("text")
  )
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

    val anotherOption = readDf(
      spark,
      "",
      format = Some("jdbc"),
      options = Map(
        "driver"   -> driver,
        "url"      -> dbUrl,
        "user"     -> username,
        "password" -> password,
        "dbtable"  -> "public.employees"
      )
    )

    val betterOption = spark.read
      .jdbc(
        url = dbUrl,
        table = "public.employees",
        properties = properties
      )

    anotherOption
  }

  employeesDf.show()

  /**
    * Exercise: Read the movies DF and then write it as
    *  - tab-separated values files
    *  - snappy parquet
    *  - table "public.movies" in the Postgres DB
    */
  val moviesDf = readDf(spark, "movies.json")

  // 1. TSVs
  writeDf(
    moviesDf,
    spark,
    "movies",
    "csv",
    options = Map(
      "header" -> "true",
      "sep"    -> "\t"
//        "nullValue" -> "" // default
    )
  )

  // moviesDf.write
  //    .mode(SaveMode.Overwrite)
  //    .option("header", "true")
  //    .option("sep", "\t")
  //    .csv(dataPath("movies", "csv"))

  // 2. Snappy parquet
  writeDf(
    moviesDf,
    spark,
    "movies",
    "parquet"
//    options = Map(
//      "compression" -> "snappy" // Default is snappy
//    )
  )

//  moviesDf.write
//    .mode(SaveMode.Overwrite)
//    .save(dataPath("movies", "parquet"))

  // 3. Postgres DB table
  writeDf(
    moviesDf,
    spark,
    "movies",
    "jdbc",
    options = Map(
      "driver"   -> driver,
      "url"      -> dbUrl,
      "user"     -> username,
      "password" -> password,
      "dbtable"  -> "public.movies"
    )
  )

//  moviesDf.write
//    .mode(SaveMode.Overwrite)
//    .jdbc(
//      url = dbUrl,
//      table = "public.movies",
//      connectionProperties = properties
//    )
}
