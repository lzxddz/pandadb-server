package cn.pandadb.driver.values


trait Value extends Serializable {

  def getType(): String = { throw new NotImplementException()}

  def isNull(): Boolean =  { throw new NotImplementException()}

  def asAny(): Any = { throw new NotImplementException()}
  def asInt(): Int = { throw new NotImplementException()}
  def asLong(): Long = { throw new NotImplementException()}
  def asBoolean(): Boolean = { throw new NotImplementException()}
  def asFloat(): Float = { throw new NotImplementException()}
  def asString(): String = { throw new NotImplementException()}
  def asNode(): Node = { throw new NotImplementException()}

  override def toString: String = "Driver Base Value"
}

class NotImplementException(e: String="not implement error") extends Exception(e) {

}

class AnyValue (value: Any) extends Value {
  override def getType(): String = Types.ANY.toString
  override def asAny(): Any =  value
  override def toString(): String = "Driver AnyValue"
}

class StringValue (value: String) extends Value {
  override def getType(): String = Types.STRING.toString
  override def asAny(): Any =  value
  override def asString(): String = value

  override def toString: String = value
}

class IntegerValue (value: Long) extends Value {
  override def getType(): String = Types.INTEGER.toString
  override def asAny(): Any =  value
  override def asInt(): Int = value.asInstanceOf[Int]
  override def asLong(): Long = value

  override def toString: String = value.toString

}

class FloatValue (value: Float) extends Value {
  override def getType(): String = Types.INTEGER.toString
  override def asAny(): Any =  value
  override def asFloat(): Float = value

  override def toString: String = value.toString
}

class BooleanValue (value: Boolean) extends Value {
  override def getType(): String = Types.INTEGER.toString
  override def asAny(): Any =  value
  override def asBoolean(): Boolean = value

  override def toString: String = value.toString
}

object NullValue extends Value {
  val value = null
  override def getType(): String = Types.INTEGER.toString
  override def asAny(): Any =  value
  override def isNull: Boolean = true

  override def toString(): String = {"Driver NullValue"}
}

class NodeValue(value: Node) extends Value {
  override def getType(): String = Types.INTEGER.toString
  override def asAny(): Any =  value
  override def asNode(): Node = value

  override def toString(): String = value.toString
}