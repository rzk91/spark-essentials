package common.implicits

import org.apache.spark.sql.types.{DataType, Metadata, StructType}

class SparkSchemaOps(private val schema: StructType) extends AnyVal {

  def dropField(name: String): StructType = StructType(schema.filterNot(_.name == name))

  def changeType(
    name: String,
    newType: DataType
  ): StructType = {
    val modded = schema(name)
    dropField(name).add(name, newType, modded.nullable, modded.metadata)
  }
}
