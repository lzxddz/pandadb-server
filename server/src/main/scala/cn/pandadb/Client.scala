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
    print("Result1:   ")
    println(result1.records)

    val cypher2 = "match (n) return n.age"
    val result2 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher2))
    print("Result2:   ")
    println(result2.records)

    val cypher3 = "match (n) return n limit 2"
    val result3 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher3))
    print("Result3:   ")
    println(result3.records)
//    println(result.getResults())
//    val result = endPointRef.askWithRetry[mutable.Buffer[Map[String, AnyRef]]](RunQuery(cypher))
//    println(result)
    val cypher4 = "match (n) return n.name"
    val result4 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher4))
    print("Result4:   ")
    println(result4.records)

    val cypher5 = "match (n) return n.adult"
    val result5 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher5))
    print("Result5:   ")
    println(result5.records)

    val cypher6 = "match (n) return n.born"
    val result6 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher6))
    print("Result6:   ")
    println(result6.records)

    val cypher7 = "match (n) return n.born1"
    val result7 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher7))
    print("Result7:   ")
    println(result7.records)

    val cypher8 = "match (n) return n.born2"
    val result8 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher8))
    print("Result8:   ")
    println(result8.records)

    val cypher9 = "match (n) return n.born3"
    val result9 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher9))
    print("Result9:   ")
    println(result9.records)

    val cypher10 = "match (n) return n.born4"
    val result10 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher10))
    print("Result10:   ")
    println(result10.records)

//    val cypher11 = "match (n) return n.dur"
//    val result11 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher11))
//    println(result11.records)

    val cypher12 = "match (n:Person {name:'test07', age:10})-[r]->(neo:Company{business:'Software'}) return r"
    val result12 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher12))
    print("Result12:   ")
    println(result12.records)

    val cypher13 = "match (n:Person {name:'test07', age:10})-[r]->(neo:Company{business:'Software'}) return r.name"
    val result13 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher13))
    print("Result13:   ")
    println(result13.records)

    val cypher14 = "match (n:Person {name:'test07', age:10})-[r]->(neo:Company{business:'Software'}) return r.age"
    val result14 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher14))
    print("Result14:   ")
    println(result14.records)

    val cypher15 = "match (n:Person {name:'test07', age:10})-[r]->(neo:Company{business:'Software'}) return r.born"
    val result15 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher15))
    print("Result15:   ")
    println(result15.records)

//    val cypher16 = "match p=(vic:Person{name:'vic'})-[]->(michael:Person{name:'Michael'}) return p"
//    val result16 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher16))
//    println(result16.records)

    val cypher17 = "match (n:Person) return n"
    val result17 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher17))
    print("Result17:   ")
    println(result17.records)

    val cypher18 = "match (n:Teacher) return n"
    val result18 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher18))
    print("Result18:   ")
    println(result18.records)

    val cypher19 = "match (n:Person:Student) return n"
    val result19 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher19))
    print("Result19:   ")
    println(result19.records)

    val cypher20 = "match (n:Person{name: 'test01'}) return n"
    val result20 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher20))
    print("Result20:   ")
    println(result20.records)

    val cypher21 = "match (n) where n.name='test01' return n"
    val result21 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher21))
    print("Result21:   ")
    println(result21.records)

    val cypher22 = "match (n:Teacher) where n.age<35 and n.sex='male' return n"
    val result22 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher22))
    print("Result22:   ")
    println(result22.records)

    val cypher23 = "match (n:Person{name: 'test05'}) return n.sex, n.age, n.class, n.school"
    val result23 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher23))
    print("Result23:   ")
    println(result23.records)
  }
}
