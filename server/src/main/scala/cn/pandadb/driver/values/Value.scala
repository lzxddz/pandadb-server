package cn.pandadb.driver.values

import java.time.temporal.{Temporal, TemporalAmount, TemporalUnit}
import java.time.{Duration, LocalDate, LocalDateTime, LocalTime, OffsetTime, ZonedDateTime}
import java.util

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
  def asBytes(): Byte = { throw new NotImplementException()}
  def asNumber(): Number = { throw new NotImplementException()}
  def asDate(): LocalDate = { throw new NotImplementException()}
  def asTime(): OffsetTime = { throw new NotImplementException()}
  def asDateTime(): ZonedDateTime = { throw new NotImplementException()}
  def asLocalTime(): LocalTime = { throw new NotImplementException()}
  def asLocalDateTime(): LocalDateTime = { throw new NotImplementException()}
  def asDuration(): Duration = { throw new NotImplementException()}
  def asPoint(): Point = { throw new NotImplementException()}
  def asList[T](): List[T] = { throw new NotImplementException()}
  def asMap[K,V](): Map[K,V] = { throw new NotImplementException()}
  def asRelationship(): Relationship = { throw new NotImplementException()}
  def asPath(): Path = { throw new NotImplementException()}

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
  override def getType(): String = Types.FLOAT.toString
  override def asAny(): Any =  value
  override def asFloat(): Float = value

  override def toString: String = value.toString
}

class BooleanValue (value: Boolean) extends Value {
  override def getType(): String = Types.BOOLEAN.toString
  override def asAny(): Any =  value
  override def asBoolean(): Boolean = value.asInstanceOf[Boolean]

  override def toString: String = value.toString
}

object NullValue extends Value {
  val value = null
  override def getType(): String = Types.NULL.toString
  override def asAny(): Any =  value
  override def isNull: Boolean = true

  override def toString(): String = {"Driver NullValue"}
}

class NodeValue(value: Node) extends Value {
  override def getType(): String = Types.NODE.toString
  override def asAny(): Any =  value
  override def asNode(): Node = value

  override def toString(): String = value.toString
}

class BytesValue(value: Byte) extends Value {
  override def getType(): String = Types.BYTES.toString
  override def asAny(): Any = value
  override def asBytes(): Byte = value

  override def toString: String = value.toString
}

class NumberValue(value: Number) extends Value {
  override def getType(): String = Types.NUMBER.toString
  override def asAny(): Any = value
  override def asNumber(): Number = value

  override def toString: String = value.toString
}

class DateValue(value: LocalDate) extends Value {
  override def getType(): String = Types.DATE.toString
  override def asAny(): Any = value
  override def asDate(): LocalDate = value

  override def toString: String = value.toString
}

class TimeValue(value: OffsetTime) extends Value {
  override def getType(): String = Types.TIME.toString
  override def asAny(): Any = value
  override def asTime(): OffsetTime = value

  override def toString: String = value.toString
}

class DateTimeValue(value: ZonedDateTime) extends Value {
  override def getType(): String = Types.DATE_TIME.toString
  override def asAny(): Any = value
  override def asDateTime(): ZonedDateTime = value

  override def toString: String = value.toString
}

class LocalTimeValue(value: LocalTime) extends Value {
  override def getType(): String = Types.LOCAL_TIME.toString
  override def asAny(): Any = value
  override def asLocalTime(): LocalTime = value

  override def toString: String = value.toString
}

class LocalDateTimeValue(value: LocalDateTime) extends Value {
  override def getType(): String = Types.LOCAL_DATE_TIME.toString
  override def asAny(): Any = value
  override def asLocalDateTime(): LocalDateTime = value

  override def toString: String = value.toString
}

class DurationValue(value: Duration) extends Value {
  override def getType(): String = Types.DURATION.toString
  override def asAny(): Any = value
  override def asDuration(): Duration = value

  override def toString: String = DurationValue.super.toString
}

class PointValue(value: Point) extends Value {
  override def getType(): String = Types.POINT.toString
  override def asAny(): Any = value
  override def asPoint(): Point = value

  override def toString: String = value.toString
}

//class ListValue[T](value: List[T]) extends Value {
//  val list = value
//  override def getType(): String = Types.LIST.toString
//  override def asAny(): Any = value
//  override def asList[T]() = list
//
//  override def toString: String = value.toString
//}
//
//class MapValue[K,V](value: Map[K,V]) extends Value {
//  override def getType(): String = Types.MAP.toString
//  override def asAny(): Any = value
//  override def asMap[K,V]() = value
//
//  override def toString: String = value.toString
//}

class RelationshipValue(value: Relationship) extends Value {
  override def getType(): String = Types.RELATIONSHIP.toString
  override def asAny(): Any = value
  override def asRelationship(): Relationship = value

  override def toString: String = value.toString
}

class PathValue(value: Path) extends Value {
  override def getType(): String = Types.PATH.toString
  override def asAny(): Any = value
  override def asPath(): Path = value

  override def toString: String = value.toString
}