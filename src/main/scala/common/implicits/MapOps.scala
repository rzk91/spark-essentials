package common.implicits

import java.util.Properties

class MapOps[K, V](private val map: Map[K, V]) extends AnyVal {

  def asProperties: Properties = {
    val props = new Properties()
    map.foreach { case (k, v) => props.put(k, v) }
    props
  }

}
