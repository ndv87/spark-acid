/*
 * Copyright 2019 Qubole, Inc.  All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import scala.collection.JavaConverters._
import com.qubole.spark.hiveacid.hive.HiveAcidMetadata
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTablePartition, CatalogUtils}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, BoundReference, Expression, Nondeterministic}
import org.apache.spark.sql.catalyst.trees.TreeNode

import scala.collection.Seq
import scala.util.control.NonFatal

object HiveAcidUtils {

  /**
    * This is adapted from [[org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.prunePartitionsByFilter]]
    * Instead of [[org.apache.spark.sql.catalyst.catalog.CatalogTable]] this function will be using [[HiveAcidMetadata]]
    * @param hiveAcidMetadata
    * @param inputPartitions
    * @param predicates
    * @param defaultTimeZoneId
    * @return
    */
  def prunePartitionsByFilter(
                               hiveAcidMetadata: HiveAcidMetadata,
                               inputPartitions: Seq[CatalogTablePartition],
                               predicates: Option[Expression],
                               defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    if (predicates.isEmpty) {
      inputPartitions
    } else {
      val partitionSchema = hiveAcidMetadata.partitionSchema
      val partitionColumnNames = hiveAcidMetadata.partitionSchema.fieldNames.toSet

      val nonPartitionPruningPredicates = predicates.filterNot {
        _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
      }
      if (nonPartitionPruningPredicates.nonEmpty) {
        throw new AnalysisException("Expected only partition pruning predicates: " +
          nonPartitionPruningPredicates)
      }

      val boundPredicate =
        InterpretedPredicate.create(predicates.get.transform {
          case att: Attribute =>
            val index = partitionSchema.indexWhere(_.name == att.name)
            BoundReference(index, partitionSchema(index).dataType, nullable = true)
        })

      inputPartitions.filter { p =>
        boundPredicate.eval(p.toRow(partitionSchema, defaultTimeZoneId))
      }
    }
  }

  def convertToCatalogTablePartition(hp: com.qubole.shaded.hadoop.hive.ql.metadata.Partition): CatalogTablePartition = {
    val apiPartition = hp.getTPartition
    val properties: Map[String, String] = if (hp.getParameters != null) {
      hp.getParameters.asScala.toMap
    } else {
      Map.empty
    }
    CatalogTablePartition(
      spec = Option(hp.getSpec).map(_.asScala.toMap).getOrElse(Map.empty),
      storage = CatalogStorageFormat(
        locationUri = Option(CatalogUtils.stringToURI(apiPartition.getSd.getLocation)),
        inputFormat = Option(apiPartition.getSd.getInputFormat),
        outputFormat = Option(apiPartition.getSd.getOutputFormat),
        serde = Option(apiPartition.getSd.getSerdeInfo.getSerializationLib),
        compressed = apiPartition.getSd.isCompressed,
        properties = Option(apiPartition.getSd.getSerdeInfo.getParameters)
          .map(_.asScala.toMap).orNull),
      createTime = apiPartition.getCreateTime.toLong * 1000,
      lastAccessTime = apiPartition.getLastAccessTime.toLong * 1000,
      parameters = properties,
      stats = None) // TODO: need to implement readHiveStats
  }
}

object InterpretedPredicate {
//  def create(expression: Expression, inputSchema: Seq[Attribute]): InterpretedPredicate =
//    create(BindReferences.bindReference(expression, inputSchema))

  def create(expression: Expression): InterpretedPredicate = new InterpretedPredicate(expression)
}

case class InterpretedPredicate(expression: Expression) extends BasePredicate {
  override def eval(r: InternalRow): Boolean = expression.eval(r).asInstanceOf[Boolean]

  override def initialize(partitionIndex: Int): Unit = {
    super.initialize(partitionIndex)
    expression.foreach {
      case n: Nondeterministic => n.initialize(partitionIndex)
      case _ =>
    }
  }
}

object BindReferences extends Logging {

  def bindReference[A <: Expression](
                                      expression: A,
                                      input: AttributeSeq,
                                      allowFailures: Boolean = false): A = {
    expression.transform { case a: AttributeReference =>
      attachTree(a, "Binding attribute") {
        val ordinal = input.indexOf(a.exprId)
        if (ordinal == -1) {
          if (allowFailures) {
            a
          } else {
            sys.error(s"Couldn't find $a in ${input.attrs.mkString("[", ",", "]")}")
          }
        } else {
          BoundReference(ordinal, a.dataType, input(ordinal).nullable)
        }
      }
    }.asInstanceOf[A] // Kind of a hack, but safe.  TODO: Tighten return type when possible.
  }

  def attachTree[TreeType <: TreeNode[_], A](tree: TreeType, msg: String = "")(f: => A): A = {
    try f catch {
      // SPARK-16748: We do not want SparkExceptions from job failures in the planning phase
      // to create TreeNodeException. Hence, wrap exception only if it is not SparkException.
      case NonFatal(e) if !e.isInstanceOf[SparkException] =>
        throw new TreeNodeException(tree, msg, e)
    }
  }
}

class TreeNodeException[TreeType <: TreeNode[_]](
                                                  @transient val tree: TreeType,
                                                  msg: String,
                                                  cause: Throwable)
  extends Exception(msg, cause) {

  val treeString = tree.toString

  // Yes, this is the same as a default parameter, but... those don't seem to work with SBT
  // external project dependencies for some reason.
  def this(tree: TreeType, msg: String) = this(tree, msg, null)

  override def getMessage: String = {
    s"${super.getMessage}, tree:${if (treeString contains "\n") "\n" else " "}$tree"
  }
}

abstract class BasePredicate {
  def eval(r: InternalRow): Boolean

  /**
   * Initializes internal states given the current partition index.
   * This is used by nondeterministic expressions to set initial states.
   * The default implementation does nothing.
   */
  def initialize(partitionIndex: Int): Unit = {}
}