package cn.pandadb

import java.io.File
import java.{util => javautil}

import scala.collection.JavaConverters._

import org.neo4j.graphdb.{GraphDatabaseService, Result}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.kernel.impl.core.NodeProxy
import org.neo4j.values.storable.IntegralArray

object Test1 {

  def main(args: Array[String]): Unit = {
    val db:GraphDatabaseService =  new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(
      new File("./output/testdb")).newGraphDatabase()
//    val r1:Result = db.execute("create (n:person{name:'t3',age:12,arr:[1,2,3]}) return n, n.arr")
//    println(r1.next().get("n.arr").getClass.getTypeName)
//    println(r1.next().get("n.arr").isInstanceOf[Array[Long]])
//    println(r1.columns())
//    val n1: NodeProxy = r1.next().get("n.arr").asInstanceOf[NodeProxy]
//    println(n1)
    val r2 = db.execute("match (n) return n.age;")
//    println(r2.columns())
    while (r2.hasNext){
    val v1 = r2.next().get("n.age")
      println(v1 == null)
//      println(v1.isInstanceOf[Long], v1.asInstanceOf[Long])
    }

  }

}
