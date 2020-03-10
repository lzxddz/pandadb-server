package cn.pandadb.util

import cn.pandadb.driver.values.{Duration, Node, NodeValue, Relationship, RelationshipValue, Value}
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetTime, ZonedDateTime}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import org.neo4j.graphdb.{Result => Neo4jResult}
import cn.pandadb.driver.result.{InternalRecords, Record}
import cn.pandadb.driver.values
import org.neo4j.kernel.impl.core.{NodeProxy, PathProxy, RelationshipProxy}

import scala.collection.mutable.ArrayBuffer

/*
* 用于将neo4j中的Value转成driver中的Value
* */

object ValueConverter {

  def convertValue(v: Any): Value = {
    if (v.isInstanceOf[NodeProxy]) { convertNode(v.asInstanceOf[NodeProxy])}
    else if (v.isInstanceOf[Int]) { new values.IntegerValue(v.asInstanceOf[Int]) }
    else if (v.isInstanceOf[Long]) { new values.IntegerValue(v.asInstanceOf[Long]) }
    else if (v.isInstanceOf[String]) { new values.StringValue(v.asInstanceOf[String]) }
    else if (v.isInstanceOf[Float]) { new values.FloatValue(v.asInstanceOf[Float]) }
    else if (v.isInstanceOf[Boolean]) { new values.BooleanValue(v.asInstanceOf[Boolean]) }
    else if (v.isInstanceOf[Number]) { new values.NumberValue(v.asInstanceOf[Number]) }
    else if (v.isInstanceOf[Duration]) { convertDuration(v.asInstanceOf[Duration]) }
    else if (v.isInstanceOf[LocalDate]) { new values.DateValue(v.asInstanceOf[LocalDate]) }
    else if (v.isInstanceOf[OffsetTime]) { new values.TimeValue(v.asInstanceOf[OffsetTime]) }
    else if (v.isInstanceOf[ZonedDateTime]) { new values.DateTimeValue(v.asInstanceOf[ZonedDateTime]) }
    else if (v.isInstanceOf[LocalTime]) { new values.LocalTimeValue(v.asInstanceOf[LocalTime]) }
    else if (v.isInstanceOf[LocalDateTime]) { new values.LocalDateTimeValue(v.asInstanceOf[LocalDateTime]) }
    else if (v.isInstanceOf[RelationshipProxy]) { convertRelationship(v.asInstanceOf[RelationshipProxy])}
    else if (v.isInstanceOf[PathProxy]) {convertPath(v.asInstanceOf[PathProxy])}
    else if (v == null) { values.NullValue }
    else {new values.AnyValue(v)}
  }

  def convertNode(node: NodeProxy): values.NodeValue = {
    val id = node.getId
    val props: mutable.Map[String, AnyRef] = node.getAllProperties().asScala
    val propsMap = new mutable.HashMap[String, Value]()
    for(k <- props.keys) {
      val v1 = props.get(k).getOrElse(null)
      val v: Value = convertValue(v1)
      propsMap(k) = v
    }
    val node1 = new values.Node(id, propsMap.toMap)
    new values.NodeValue(node1)
  }

  def convertRelationship(relationship: RelationshipProxy): values.RelationshipValue = {
    val id = relationship.getId
    val props: mutable.Map[String, AnyRef] = relationship.getAllProperties().asScala
    val propsMap = new mutable.HashMap[String, Value]()
    val startNodeId = relationship.getStartNodeId
    val endNodeId = relationship.getEndNodeId
    for(k <- props.keys) {
      val v1 = props.get(k).getOrElse(null)
      val v: Value = convertValue(v1)
      propsMap(k) = v
    }
    val rel = new values.Relationship(id, propsMap.toMap, startNodeId, endNodeId)
    new values.RelationshipValue(rel)
  }

  def convertPath(path: PathProxy): values.PathValue = {
    val nodes = path.nodes()
    val relationships = path.relationships()
    var newNodes= new ArrayBuffer[NodeValue]
    var newRelationships = new ArrayBuffer[RelationshipValue]
    for(n <- nodes)
      newNodes += convertNode(n.asInstanceOf[NodeProxy])
    for(r <- relationships)
      newRelationships += convertRelationship(r.asInstanceOf[RelationshipProxy])
    var paths = new ArrayBuffer[Value]
    var i = 0
    var ni = 0
    var ri = 0
    for ( i <- 0 to newNodes.size+newRelationships.size) {
      if (i % 2 == 0) {
        paths += newNodes(ni)
        ni += 1
      }
      else {
        paths += newRelationships(ri)
        ri += 1
      }
    }
    val path1 = new values.Path(paths.toArray)
    new values.PathValue(path1)
  }

  def convertDuration(duration: Duration): values.DurationValue = {
    val months = duration.months
    val days = duration.days
    val seconds = duration.seconds
    val nano = duration.nanoseconds
    val dur = new values.Duration(months, days, seconds, nano)
    new values.DurationValue(dur)
  }

  def neo4jResultToDriverRecords(result: Neo4jResult): InternalRecords = {
    val internalRecords = new InternalRecords()

    while (result.hasNext){
      // 将Map 转成Record
      val row: mutable.Map[String, AnyRef] = result.next().asScala
      val map1 = new mutable.HashMap[String, Value]()
      for(k <- row.keys) {
        val v1 = row.get(k).getOrElse(null)
        val v: Value = convertValue(v1)
        map1(k) = v
      }
      internalRecords.append(new Record(map1.toMap))
    }

    internalRecords
  }

}
