package cn.pandadb.driver.values

class Relationship(id: Long,
                   props: Map[String, Value],
                   startNode: Long,
                   endNode: Long) extends Serializable {
}
