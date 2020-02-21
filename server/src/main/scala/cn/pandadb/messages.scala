package cn.pandadb


case class RunQuery(cyhper: String)


class PandaResult (cols: Iterable[String], rows: Iterable[Map[String, AnyRef]] ) extends Serializable {

  def getResults(): Iterable[Map[String, AnyRef]] = {
    rows
  }
}

