package cypher.features

import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.graphdb._
import org.neo4j.test.TestGraphDatabaseFactory
import org.opencypher.tools.tck.api._
import org.opencypher.tools.tck.values.CypherValue

import scala.collection.JavaConverters._

object Neo4jAdapter {
  def apply(): Neo4jAdapter = {
    val instance = new GraphDatabaseCypherService(new TestGraphDatabaseFactory().newImpermanentDatabase())
    new Neo4jAdapter(instance.getGraphDatabaseService)
  }
}

class Neo4jAdapter(instance: GraphDatabaseService) extends Graph {
  override def cypher(query: String, params: Map[String, CypherValue], queryType: QueryType): Records = {
    // TODO: convert params
    val tx = instance.beginTx()
    val result: Result = instance.execute(query)
    val convertedResult = convertResult(result)
    tx.success()
    tx.close()

    if (queryType == ExecQuery) {
      println(convertedResult)
    }

    convertedResult
  }

  def convertResult(result: Result): Records = {
    val header = result.columns().asScala.toList
    val rows: List[Map[String, String]] = result.asScala.map { row =>
      row.asScala.map { case (k, v) => (k, convertValue(v)) }.toMap
    }.toList
    StringRecords(header, rows)
  }

  def convertValue(value: Any): String = {
    value match {
      case n: Node =>
        val labels = n.getLabels.asScala.map(_.name()).toList
        val properties = convertValue(n.getAllProperties)
        s"(${labels.mkString(":", ":", " ")}$properties)"

      case r: Relationship =>
        val relType = r.getType.name()
        val properties = convertValue(r.getAllProperties)
        s"[:$relType$properties]"

      case m: java.util.Map[String, _] =>
        val properties = m.asScala.map {
          case (k, v) => (k, convertValue(v))
        }
        s"{${
          properties.map {
            case (k, v) => s"$k: $v"
          }.mkString(", ")
        }}"
      case l: java.util.List[_] =>
        val convertedElements = l.asScala.map(convertValue)
        s"[${convertedElements.mkString(", ")}]"

      case path: Path =>
        val (string, _) = path.relationships().asScala.foldLeft((convertValue(path.startNode()), path.startNode().getId)) {
          case ((currentString, currentNodeId), nextRel) =>
            if (currentNodeId == nextRel.getStartNodeId) {
              val updatedString = s"$currentString-${convertValue(nextRel)}->${convertValue(nextRel.getEndNode)}"
              updatedString -> nextRel.getEndNodeId
            } else {
              val updatedString = s"$currentString<-${convertValue(nextRel)}-${convertValue(nextRel.getStartNode)}"
              updatedString -> nextRel.getStartNodeId
            }
        }
        s"<$string>"
      case s: String => s"'$s'"
      case l: Long => l.toString
      case other =>
        if (other == null) {
          "null"
        } else {
          println(s"could not convert $other of type ${other.getClass}")
          other.toString
        }
    }
  }
}
