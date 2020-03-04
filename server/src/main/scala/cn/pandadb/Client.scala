package cn.pandadb

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory

import cn.pandadb.driver.result.InternalRecords


object Client {

  def main(args: Array[String]): Unit = {
    //    asyncCall()
    syncCall()
  }

  def asyncCall() = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "panda-client")
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 52345), "panda-service")
    val future: Future[String] = endPointRef.ask[String](RunQuery("neo"))
    future.onComplete {
      case scala.util.Success(value) => println(s"Got the result = $value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }
    Await.result(future, Duration.apply("30s"))
  }

  def syncCall() = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "panda-client")
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 52345), "panda-service")


    val cypher1 = "match (n) return count(n)"
    val result1 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher1))
    println(result1.records)

    val cypher2 = "match (n) return n.age"
    val result2 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher2))
    println(result2.records)

    val cypher3 = "match (n) return n limit 2"
    val result3 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher3))
    println(result3.records)
//    println(result.getResults())
//    val result = endPointRef.askWithRetry[mutable.Buffer[Map[String, AnyRef]]](RunQuery(cypher))
//    println(result)

  }
}
