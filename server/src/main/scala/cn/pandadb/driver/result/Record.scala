package cn.pandadb.driver.result

import scala.collection.immutable.Map
import cn.pandadb.driver.values.{Value, NullValue}

class Record (private val container: Map[String, Value]) extends Serializable {
  def keys(): Iterable[String] = container.keys
  def containsKey(key: String): Boolean = container.contains(key)
  def get(key: String): Value = container.getOrElse(key, NullValue)

  override def toString: String = container.toString().toString
}
