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

package com.qubole.spark.hiveacid

import com.qubole.spark.datasources.hiveacid.sql.catalyst.plans.command.{DeleteCommand, MergeCommand, UpdateCommand}
import com.qubole.spark.hiveacid.datasource.HiveAcidDataSource.getFullyQualifiedTableName

import java.util.Locale
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.{DeleteAction, DeleteFromTable, Filter, InsertAction, InsertIntoStatement, InsertStarAction, LogicalPlan, MergeIntoTable, SubqueryAlias, UpdateAction, UpdateStarAction, UpdateTable, View}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import com.qubole.spark.hiveacid.datasource.{HiveAcidDataSource, HiveAcidRelation}
import com.qubole.spark.hiveacid.merge.{MergeCondition, MergeWhenClause, MergeWhenDelete, MergeWhenNotInsert, MergeWhenUpdateClause}
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.{Table, TableCatalog}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf

import scala.collection.immutable.Seq
import scala.util.Try


/**
 * Analyzer rule to convert a transactional HiveRelation
 * into LogicalRelation backed by HiveAcidRelation
 * @param spark - spark session
 */
case class HiveAcidAutoConvert(spark: SparkSession) extends Rule[LogicalPlan] {

  private def isConvertible(relation: HiveTableRelation): Boolean = {
    val serde = relation.tableMeta.storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
    relation.tableMeta.properties.getOrElse("transactional", "false").toBoolean
  }

  private def convert(relation: HiveTableRelation): LogicalRelation = {
    val options = relation.tableMeta.properties ++
      relation.tableMeta.storage.properties ++ Map("table" -> relation.tableMeta.qualifiedName)

    val currentCatalog = spark.sessionState.catalogManager.currentCatalog.name()
    val table = spark.sessionState.sqlParser
      .parseTableIdentifier(getFullyQualifiedTableName(options).replaceAll(s"$currentCatalog\\.", ""))

    val tb = spark.sessionState.catalog.getTableMetadata(table)
    val newRelation = new HiveAcidDataSource().createRelation(spark.sqlContext, options)
    val lr = LogicalRelation(newRelation, tb)
    lr
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val p = plan resolveOperators {
      // Write path
      case InsertIntoStatement (r: HiveTableRelation, partition, userSpecifiedCols, query, overwrite, ifPartitionNotExists)
        if query.resolved && DDLUtils.isHiveTable(r.tableMeta) && isConvertible(r) =>
        InsertIntoStatement (convert(r), partition, userSpecifiedCols, query, overwrite, ifPartitionNotExists)
      // Read path
      case relation: HiveTableRelation
        if DDLUtils.isHiveTable(relation.tableMeta) && isConvertible(relation) =>
        convert(relation)
    }
    p
    p resolveOperatorsDown {
      case u @ UpdateTable (EliminatedSubQuery(aliasedTable), assignments, condition) =>
        val setExpressions = assignments.map(kv=> kv.key.sql -> kv.value).toMap
        UpdateCommand(aliasedTable, setExpressions, condition)
      case u @ DeleteFromTable (EliminatedSubQuery(aliasedTable), condition) =>
        DeleteCommand(aliasedTable, condition)
      case u @ MergeIntoTable (target, source, cond, matchedActions, notMatchedActions) =>

      if (EliminatedSubQuery.unapply(target).isEmpty) {
        u
      } else {
        val unaliasedTarget = EliminatedSubQuery.unapply(target).get
        val unaliasedSource = EliminatedSubQuery.unapply(source).get
        val targetAllias = extractAlias(target)
        val sourceAllias = extractAlias(source)

        val matched: Seq[MergeWhenClause] = matchedActions.map {
          case DeleteAction(condition) => MergeWhenDelete(condition)
          case UpdateAction(condition, assignments) =>
            val setExpressions = assignments.map(kv => kv.key.sql -> kv.value).toMap
            MergeWhenUpdateClause(condition, setExpressions, false)
          case UpdateStarAction(condition) =>
            MergeWhenUpdateClause(condition, Map.empty, true)
        }

        val notMatched = notMatchedActions.map {
          case InsertAction(condition, assignments) =>
            val setExpressions = assignments.map(_.key)
            MergeWhenNotInsert(condition, setExpressions)
          case InsertStarAction(condition) => MergeWhenNotInsert(condition, Seq(expr("*").expr))
        }.headOption

        MergeCommand(unaliasedTarget, unaliasedSource, matched, notMatched, MergeCondition(cond), sourceAllias, targetAllias)
      }
    }
  }

  def extractAlias(plan: LogicalPlan) = {
    plan match {
      case SubqueryAlias(identifier, child) =>
        Some(identifier)
      case _ =>
        None
    }
  }
}

object EliminatedSubQuery {

  def unapply(plan: LogicalPlan): Option[LogicalPlan] = {
    val r = EliminateSubqueryAliases(plan)
    r match {
      case View(_, _, plan) =>
        Some(plan)
      case LogicalRelation(r: HiveAcidRelation, _, _, _) =>
        Some(plan)
      case _ =>
        None
    }
  }



}

class HiveAcidAutoConvertExtension extends (SparkSessionExtensions => Unit) {
  def apply(extension: SparkSessionExtensions): Unit = {

    extension.injectResolutionRule{ spark => HiveAcidAutoConvert(spark) }

  }
}
