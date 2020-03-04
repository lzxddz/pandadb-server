package cn.pandadb.util

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import org.neo4j.graphdb.{Result => Neo4jResult}
import cn.pandadb.driver.result.{InternalRecords, Record}
import cn.pandadb.driver.values
import cn.pandadb.driver.values.Value
import org.neo4j.kernel.impl.core.NodeProxy

/*
* 用于将neo4j中的Value转成driver中的Value
* */

object ValueConverter {

  def convertValue(v: Any): Value = {
    if (v.isInstanceOf[NodeProxy]) {convertNode(v.asInstanceOf[NodeProxy])}
    else if (v.isInstanceOf[Int]) { new values.IntegerValue(v.asInstanceOf[Int]) }
    else if (v.isInstanceOf[Long]) { new values.IntegerValue(v.asInstanceOf[Long]) }
    else if (v.isInstanceOf[String]) { new values.StringValue(v.asInstanceOf[String]) }
    else if (v.isInstanceOf[Float]) { new values.FloatValue(v.asInstanceOf[Float]) }
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
