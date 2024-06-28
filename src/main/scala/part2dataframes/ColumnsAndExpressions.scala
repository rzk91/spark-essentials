package part2dataframes

import common.SparkCommon

object ColumnsAndExpressions extends App with SparkCommon {

  val spark = simpleSparkSession("ColumnsAndExpressions")

  val carsSchema = sampleSchema

  val carsDf = readDf(spark, "cars.json", Some(carsSchema))

  carsDf.show()

}
