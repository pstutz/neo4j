/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.features

import cypher.feature.steps.ProcedureSignature
import org.neo4j.collection.RawIterator
import org.neo4j.cypher.internal.util.v3_4.symbols.{CTBoolean, CTFloat, CTInteger, CTMap, CTNode, CTNumber, CTPath, CTRelationship, CTString, CypherType, ListType}
import org.neo4j.kernel.api.InwardKernel
import org.neo4j.kernel.api.exceptions.ProcedureException
import org.neo4j.kernel.api.proc.CallableProcedure.BasicProcedure
import org.neo4j.kernel.api.proc.{Context, Neo4jTypes}
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.procedure.Mode
import org.opencypher.tools.tck.api.{CypherValueRecords, Graph, ProcedureSupport}
import org.opencypher.tools.tck.values.CypherValue

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait Neo4jProcedureAdapter extends ProcedureSupport {
  self: Graph =>

  protected def instance: GraphDatabaseAPI

  override def registerProcedure(signature: String, values: CypherValueRecords): Unit = {
    val parsedSignature = ProcedureSignature.parse(signature)
    val kernelProcedure = buildProcedure(parsedSignature, values)
    Try(instance.getDependencyResolver.resolveDependency(classOf[InwardKernel]).registerProcedure(kernelProcedure)) match {
      case Success(_) =>
      case Failure(e) => System.err.println(s"\nRegistration of procedure $signature failed: " + e.getMessage)
    }
  }

  private def buildProcedure(parsedSignature: ProcedureSignature, values: CypherValueRecords) = {
    val signatureFields = parsedSignature.fields
    val tableColumns = values.header
    val tableValues: Seq[Array[AnyRef]] = values.rows.map {
      row: Map[String, CypherValue] =>
        tableColumns.map { columnName =>
          val value = row(columnName)
          val convertedValue = TCKValueToNeo4jValue(value)
          convertedValue
        }.toArray
    }
    if (tableColumns != signatureFields)
      throw new scala.IllegalArgumentException(
        s"Data table columns must be the same as all signature fields (inputs + outputs) in order (Actual: $tableColumns Expected: $signatureFields)"
      )
    val kernelSignature = asKernelSignature(parsedSignature)
    val kernelProcedure = new BasicProcedure(kernelSignature) {
      override def apply(ctx: Context, input: Array[AnyRef]): RawIterator[Array[AnyRef], ProcedureException] = {
        val scalaIterator = tableValues
          .filter { row => input.indices.forall { index => row(index) == input(index) } }
          .map { row => row.drop(input.length).clone() }
          .toIterator

        val rawIterator = RawIterator.wrap[Array[AnyRef], ProcedureException](scalaIterator.asJava)
        rawIterator
      }
    }
    kernelProcedure
  }

  private def asKernelSignature(parsedSignature: ProcedureSignature): org.neo4j.kernel.api.proc.ProcedureSignature = {
    val builder = org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature(parsedSignature.namespace.toArray, parsedSignature.name)
    builder.mode(Mode.READ)
    parsedSignature.inputs.foreach { case (name, tpe) => builder.in(name, asKernelType(tpe)) }
    parsedSignature.outputs match {
      case Some(fields) => fields.foreach { case (name, tpe) => builder.out(name, asKernelType(tpe)) }
      case None => builder.out(org.neo4j.kernel.api.proc.ProcedureSignature.VOID)
    }
    builder.build()
  }

  private def asKernelType(tpe: CypherType):  Neo4jTypes.AnyType = tpe match {
    case CTMap => Neo4jTypes.NTMap
    case CTNode => Neo4jTypes.NTNode
    case CTRelationship => Neo4jTypes.NTRelationship
    case CTPath => Neo4jTypes.NTPath
    case ListType(innerTpe) => Neo4jTypes.NTList(asKernelType(innerTpe))
    case CTString => Neo4jTypes.NTString
    case CTBoolean => Neo4jTypes.NTBoolean
    case CTNumber => Neo4jTypes.NTNumber
    case CTInteger => Neo4jTypes.NTInteger
    case CTFloat => Neo4jTypes.NTFloat
  }
}