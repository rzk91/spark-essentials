package common

import org.apache.spark.sql.types.StructType

import scala.language.implicitConversions

package object implicits {

  @inline implicit def enrichMaps[K, V](map: Map[K, V]): MapOps[K, V] = new MapOps(map)
  @inline implicit def enrichSparkSchema(schema: StructType): SparkSchemaOps =
    new SparkSchemaOps(schema)
}
