package cn.pandadb

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime, ZoneId, ZoneOffset, ZonedDateTime}

import cn.pandadb.driver.result.InternalRecords
import cn.pandadb.driver.values.{BooleanValue, Duration, IntegerValue, Label, Node, Path, Point, Point2D, Point3D, Relationship, RelationshipType, StringValue, Value}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}
import org.junit.{Assert, Test}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}


class Client {
  val rpcConf = new RpcConf()
  val config = RpcEnvClientConfig(rpcConf, "panda-client")
  val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
  val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 52345), "panda-service")
//  def main(args: Array[String]): Unit = {
//    //    asyncCall()
//    syncCall()
//  }

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
    import scala.concurrent.duration.Duration
    Await.result(future, Duration.apply("30s"))
  }

  @Test
  def syncCall() = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "panda-client")
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 52345), "panda-service")
  }

  @Test
  def countTest() : Unit = {
    val cypher = "match (n) return count(n)"
    val result = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher))
    Assert.assertEquals(13, result.records(0).get("count(n)").asInt())
  }

  @Test
  def intTest() : Unit = {
    val cypher = "match (n) where n.name = 'test01' return n.age"
    val result = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher))
    Assert.assertEquals(10, result.records(0).get("n.age").asInt())
  }

  @Test
  def longTest() : Unit = {
    val cypher = "match (n) where n.name = 'test01' return n.idcard"
    val result = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher))
    Assert.assertEquals(230715199809070019L, result.records(0).get("n.idcard").asLong())
  }

  @Test
  def floatTest() : Unit = {
    val cypher = "match (n) where n.name = 'test02' return n.salary"
    val result = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher))
    Assert.assertEquals(30000.987, result.records(0).get("n.salary").asFloat(),0.01)
  }

  @Test
  def byteTest() : Unit = {

  }

  @Test
  def booleanTest() : Unit = {
    val cypher = "match (n) where n.name = 'test01' return n.adult"
    val result = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher))
    Assert.assertEquals(false, result.records(0).get("n.adult").asBoolean())
  }

  @Test
  def stringTest() : Unit = {
    val cypher = "match (n) where n.age = 40 return n.name"
    val result = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher))
    Assert.assertEquals("test06", result.records(0).get("n.name").asString())
  }

  @Test
  def dateTest() : Unit = {
    val cypher = "match (n: Person{ name:'test03'}) return n.born1"
    val result = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher))
    Assert.assertEquals(LocalDate.of(2019,1,1), result.records(0).get("n.born1").asDate())
  }

  @Test
  def timeTest() : Unit = {
    val cypher = "match (n: Person{ name:'test03'}) return n.born2"
    val result = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher))
    Assert.assertEquals(OffsetTime.of(LocalTime.of(12,5,1), ZoneOffset.UTC), result.records(0).get("n.born2").asTime())
  }

  @Test
  def dateTimeTest() : Unit = {
    val cypher1 = "match (n: Person{ name:'test03'}) return n.born3"
    val result1 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher1))
    Assert.assertEquals(ZonedDateTime.of(LocalDateTime.of(2019,1,2,12,
    5,15),ZoneId.of("Australia/Eucla")),
    result1.records(0).get("n.born3").asDateTime())

    val cypher2 = "match (n: Person{ name:'test03'}) return n.born4"
    val result2 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher2))
    Assert.assertEquals(ZonedDateTime.of(LocalDateTime.of(2015,6,24,12,
    50,35,556) , ZoneOffset.UTC),
    result2.records(0).get("n.born4").asDateTime())
  }

  @Test
  def localTimeTest() : Unit = {
    val cypher = "match (n: Person{ name:'test03'}) return n.born5"
    val result = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher))
    Assert.assertEquals(LocalTime.now(), result.records(0).get("n.born5").asLocalTime())
  }

  @Test
  def localDateTimeTest() : Unit = {
    val cypher = "match (n: Person{ name:'test03'}) return n.born6"
    val result = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher))
    Assert.assertEquals(LocalDateTime.now, result.records(0).get("n.born6").asLocalDateTime())
  }

  @Test
  def numberTest() : Unit = {
    val cypher = "match (n1:Person) return min(n1.age),max(n1.age)"
    val result = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher))
    Assert.assertEquals(40, result.records(0).get("max(n1.age)").asInt())
    Assert.assertEquals(10, result.records(0).get("min(n1.age)").asInt())
  }

  @Test
  def durationTest() : Unit = {
    import cn.pandadb.driver.values.Duration
    val cypher = "match (n) where n.name = 'test03' return n.dur"
    val result = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher))
    Assert.assertEquals(Duration(0,1,0,0), result.records(0).get("n.dur").asDuration())
  }

  @Test
  def pointTest() : Unit = {
    val cypher1 = "match (n) where n.name = 'point2d' return n.location"
    val result1 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher1))
    Assert.assertEquals(new Point2D(4326, 12.78, 56.7),result1.records(0).get("n.location").asPoint2D())

    val cypher2 = "match (n) where n.name = 'point3d' return n.location"
    val result2 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher2))
    Assert.assertEquals(new Point3D(4979, 12.78, 56.7, 100.0),result2.records(0).get("n.location").asPoint3D())
  }


  @Test
  def listTest() : Unit = {
    val cypher1 = "match (n1:Person { name:'test08'}) return n1.titles"
    val result1 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher1))
    Assert.assertEquals(mutable.Buffer[Value](StringValue("ceo"),StringValue("ui"),StringValue("dev")),
                        result1.records(0).get("n1.titles").asList())

    val cypher2 = "match (n1:Person { name:'test08'}) return n1.salaries"
    val result2 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher2))
    Assert.assertEquals(mutable.Buffer[Value](IntegerValue(10000), IntegerValue(20000), IntegerValue(30597), IntegerValue(500954)),
                        result2.records(0).get("n1.salaries").asList())

    val cypher3 = "match (n1:Person { name:'test08'}) return n1.boolattr"
    val result3 = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher3))
    Assert.assertEquals(mutable.Buffer[Value](BooleanValue(false), BooleanValue(true), BooleanValue(false), BooleanValue(true)),
                        result3.records(0).get("n1.boolattr").asList())
  }

  @Test
  def mapTest() : Unit = {

  }

  @Test
  def nodeTest() : Unit = {
    val cypher = "match (n) where n.name = 'test01' return n"
    val result = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher))
    val props: Map[String, Value] = Map("idcard"-> IntegerValue(230715199809070019L),
                                        "age" -> IntegerValue(10),
                                        "name" -> StringValue("test01"),
                                        "adult" -> BooleanValue(false))
    val label: Array[Label] = Array[Label] (Label("Person"))
    val node: Node = Node(0, props, label)
    val node1: Node = result.records(0).get("n").asNode()
    Assert.assertEquals(node,node1)
  }

  @Test
  def relationshipTest() : Unit = {
    val cypher = "match (n:Person {name:'test07', age:10})-[r]->(neo:Company{business:'Software'}) return r"
    val result = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher))
    val props: Map[String, Value] = Map("age" -> IntegerValue(10),
                                       "name" -> StringValue("test08"),
                                        "adult" -> BooleanValue(false),
                                        "born" -> StringValue("2020/03/05"))
    val startProps: Map[String, Value] = Map("age" -> IntegerValue(10),
                                             "name" -> StringValue("test07"))
    val endProps: Map[String, Value] = Map("business" -> StringValue("Software"))
    val node1 = Node(8, startProps, Array[Label] (Label("Person")))
    val node2 = Node(9, endProps, Array[Label] (Label("Company")))
    val rel1: Relationship = Relationship(1,props,node1,node2,RelationshipType("WorksAt"))
    val rel2: Relationship = result.records(0).get("r").asRelationship()
    Assert.assertEquals(rel1, rel2)
  }

  @Test
  def pathTest() : Unit = {
    val cypher = "match p=(n:Person{name:'test07'})-[]->(neo:Company{business:'Software'}) return p"
    val result = endPointRef.askWithRetry[InternalRecords](RunQuery(cypher))
    val props: Map[String, Value] = Map("age" -> IntegerValue(10),
      "name" -> StringValue("test08"),
      "adult" -> BooleanValue(false),
      "born" -> StringValue("2020/03/05"))
    val startProps: Map[String, Value] = Map("age" -> IntegerValue(10),
      "name" -> StringValue("test07"))
    val endProps: Map[String, Value] = Map("business" -> StringValue("Software"))
    val node1 = Node(8, startProps, Array {Label("Person")})
    val node2 = Node(9, endProps, Array {Label("Company")})

    val rel1: Relationship = Relationship(1,props,node1,node2,RelationshipType("WorksAt"))
    val nodes:Array[Node] = Array[Node](node1 , node2)
    val rels:Array[Relationship] = Array[Relationship](rel1)
    val path1:Path = Path(nodes,rels)
    val path2:Path = result.records(0).get("p").asPath
    Assert.assertEquals(path1, path2)
  }

}
