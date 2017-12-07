package cypher.features

import java.io.File
import java.util

import org.junit.jupiter.api.{DynamicTest, TestFactory}
import org.opencypher.tools.tck.api.{CypherTCK, Feature, Graph}

import scala.collection.JavaConverters._

class TckTest {

  @TestFactory
  def testCustomFeature(): util.Collection[DynamicTest] = {
    val fooUri = getClass.getResource("/cypher/features").toURI
    val scenarios = CypherTCK.parseFilesystemFeatures(new File(fooUri)).flatMap(_.scenarios)
    //CypherTCK.parseFilesystemFeature(new File(fooUri)).scenarios

    def createTestGraph(): Graph = Neo4jAdapter()

    val dynamicTests = scenarios.map { scenario =>
      val name = scenario.toString()
      val executable = scenario(createTestGraph())
      DynamicTest.dynamicTest(name, executable)
    }
    dynamicTests.asJavaCollection
  }

  @TestFactory
  def testFullTCK(): util.Collection[DynamicTest] = {
    val scenarios = CypherTCK.allTckScenarios

    def createTestGraph(): Graph = Neo4jAdapter()

    val dynamicTests = scenarios.map { scenario =>
      val name = scenario.toString()
      val executable = scenario(createTestGraph())
      DynamicTest.dynamicTest(name, executable)
    }
    dynamicTests.asJavaCollection
  }
}
