package cn.pandadb.driver.values

import java.time.temporal.ChronoUnit.{DAYS, MONTHS, NANOS, SECONDS}
import java.time.temporal.{Temporal, TemporalAmount, TemporalUnit, UnsupportedTemporalTypeException}
import java.util
import java.util.Arrays.asList
import java.util.Collections.unmodifiableList

trait IsoDuration extends TemporalAmount{
  def months(): Long

  def days(): Long

  def seconds(): Long

  def nanoseconds(): Int
}

class Duration(months: Long, days: Long, seconds: Long, nanoseconds: Int) extends IsoDuration {
  val SUPPORTED_UNITS: util.List[TemporalUnit] = unmodifiableList(asList(MONTHS, DAYS, SECONDS, NANOS))

  override def months(): Long = months

  override def days(): Long = days

  override def seconds(): Long = seconds

  override def nanoseconds(): Int = nanoseconds

  override def get(unit: TemporalUnit): Long = {
    if (unit eq MONTHS)  months
    else if (unit eq DAYS)  days
    else if (unit eq SECONDS)  seconds
    else if (unit eq NANOS)  nanoseconds
    else throw new UnsupportedTemporalTypeException("Unsupported unit: " + unit)
  }

  override def getUnits: util.List[TemporalUnit] = SUPPORTED_UNITS

  override def addTo(temporal: Temporal): Temporal = {
    if (months != 0) temporal = temporal.plus(months, MONTHS)
    if (days != 0) temporal = temporal.plus(days, DAYS)
    if (seconds != 0) temporal = temporal.plus(seconds, SECONDS)
    if (nanoseconds != 0) temporal = temporal.plus(nanoseconds, NANOS)
    temporal
  }

  override def subtractFrom(temporal: Temporal): Temporal = {
    if (months != 0) temporal = temporal.minus(months, MONTHS)
    if (days != 0) temporal = temporal.minus(days, DAYS)
    if (seconds != 0) temporal = temporal.minus(seconds, SECONDS)
    if (nanoseconds != 0) temporal = temporal.minus(nanoseconds, NANOS)
    temporal
  }
}
