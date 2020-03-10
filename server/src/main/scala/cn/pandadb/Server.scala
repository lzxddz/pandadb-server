package cn.pandadb

import java.io.File

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc._
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import org.neo4j.graphdb.{GraphDatabaseService, Result}
import org.neo4j.graphdb.factory.GraphDatabaseFactory

import cn.pandadb.util.ValueConverter


object Server {

  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val config = RpcEnvServerConfig(new RpcConf(), "panda-server", host, 52345)
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
    val helloEndpoint: RpcEndpoint = new PandaRpcEndpoint(rpcEnv)
    rpcEnv.setupEndpoint("panda-service", helloEndpoint)
    rpcEnv.awaitTermination()
  }
}

class PandaRpcEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {

  val db:GraphDatabaseService =  new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(
          new File("./output/testdb")).newGraphDatabase()


  override def onStart(): Unit = {
    println("start panda server rpc endpoint")
    val tx = db.beginTx()
    val query =
      """
          |CREATE (n1:Person { name:'test01', age:10, adult:False, born:'2020/03/05'})
          |CREATE (n2:Person:Man { name:'test02', age:20, adult:True})
          |CREATE (n3:Person { name:'test03',born1:date('2019-01-01'), born2:time('12:05:01'), born3:datetime('2019-01-02T12:05:15[Australia/Eucla]'), born4:datetime('2015-06-24T12:50:35.000000556Z'), dur:duration({days:1})} )
          |CREATE p =(vic:Person{ name:'vic',title:"Developer" })-[:WorksAt]->(michael:Person { name: 'Michael',title:"Manager" })
          |CREATE (n4:Person:Student{name: 'test04',age:15, sex:'male', school: 'No1 Middle School'}),
          |(n5:Person:Teacher{name: 'test05', age: 30, sex:'male', school: 'No1 Middle School', class: 'math'}),
          |(n6:Person:Teacher{name: 'test06', age: 40, sex:'female', school: 'No1 Middle School', class: 'chemistry'})
          |CREATE (n7:Person { name:'test07', age:10})-[r:WorksAt{name:'test08', age:10, adult:False, born:'2020/03/05'}]->(neo:Company{business:'Software'})
      """.stripMargin
    db.execute(query)
    tx.success()
    tx.close()
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RunQuery(cyhper) => {
      println(s"run cyhper:  $cyhper")
      val tx = db.beginTx()
      val result: Result = db.execute(cyhper)
      // Neo4j Result => PandaResult
      val cols: mutable.Buffer[String] = result.columns().asScala
      val rows: mutable.Buffer[Map[String, AnyRef]] =  mutable.Buffer[Map[String, AnyRef]] ()

      val records = ValueConverter.neo4jResultToDriverRecords(result)
      println(records)
      println("replied")
      tx.success()
      tx.close()

      context.reply(records)
      println("====")
    }
  }

  override def onStop(): Unit = {
    db.shutdown()
    println("stop panda server rp endpoint")
  }
}

