package com.qubole.spark.datasources.hiveacid.sql.execution

import com.qubole.spark.datasources.hiveacid.sql.catalyst.parser._
import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.apache.iceberg.common.DynConstructors
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.parser.{ParseErrorListener, ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Concrete parser for Hive SQL statements.
 */
case class SparkAcidSqlParser(sparkParser: ParserInterface, conf: SQLConf ) extends ParserInterface with Logging {

  override def parseExpression(sqlText: String): Expression = sparkParser.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier = sparkParser.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = sparkParser.parseFunctionIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType = sparkParser.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType = sparkParser.parseDataType(sqlText)

  private val substitutorCtor: DynConstructors.Ctor[VariableSubstitution] =
    DynConstructors.builder()
      .impl(classOf[VariableSubstitution])
      .impl(classOf[VariableSubstitution], classOf[SQLConf])
      .build()

  private lazy val substitutor = substitutorCtor.newInstance(SQLConf.get)


  private val sparkAcidAstBuilder = new SparkSqlAstBuilder(conf)

  override def parsePlan(sqlText: String): LogicalPlan = {
//    sparkParser.parsePlan(sqlText)
    try {
      parse(sqlText) { parser =>
        sparkAcidAstBuilder.visitSingleStatement(parser.singleStatement()) match {
          case plan: LogicalPlan =>
            val x = plan
            x
          case _ =>
            val x = sparkParser.parsePlan(sqlText)
            x
        }
      }
    } catch {
      case e: AcidParseException =>
        throw e.parseException
      case _: ParseException =>
        sparkParser.parsePlan(sqlText)
    }
  }

  /**
   *  An adaptation of [[org.apache.spark.sql.execution.SparkSqlParser#parse]]
   *  and [[org.apache.spark.sql.catalyst.parser.AbstractSqlParser#parse]]
   */
  protected def parse[T](sqlText: String)(toResult: SqlHiveParser => T): T = {
    val command = substitutor.substitute(sqlText)
    logDebug(s"Parsing command: $command")


    val lexer = new SqlHiveLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)
    lexer.legacy_setops_precedence_enbled = SQLConf.get.setOpsPrecedenceEnforced

    val tokenStream = new CommonTokenStream(lexer)
    val acidSpecific = checkIfAcidSpecific(tokenStream)
    tokenStream.seek(0) //reset stream to first token
    val parser = new SqlHiveParser(tokenStream)
    parser.addParseListener(PostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)
    parser.legacy_setops_precedence_enbled = SQLConf.get.setOpsPrecedenceEnforced
    try {
        parser.getInterpreter.setPredictionMode(PredictionMode.LL)
        toResult(parser)
    } catch {
        case e: ParseException if e.command.isDefined =>
          throw wrapParseException(e, acidSpecific)
        case e: ParseException =>
          throw wrapParseException(e.withCommand(command), acidSpecific)
        case e: AnalysisException =>
          val position = Origin(e.line, e.startPosition)
          val pe = new ParseException(Option(command), e.message, position, position)
          throw wrapParseException(pe, acidSpecific)
      }
    }

  /**
    * Denotes ACID Specific ParseException
    * @param parseException
    */
  class AcidParseException(val parseException: ParseException) extends Exception

  def wrapParseException(e: ParseException, acidSpecific: Boolean): Throwable = {
    if (acidSpecific) {
      new AcidParseException(e)
    } else {
      e
    }
  }
  def checkIfAcidSpecific(tokStream: TokenStream): Boolean = {
    tokStream.LA(1) match {
      case SqlHiveParser.DELETE | SqlHiveParser.MERGE | SqlHiveParser.UPDATE => true
      case _ => false
    }
  }

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = sparkParser.parseMultipartIdentifier(sqlText)

  override def parseQuery(sqlText: String): LogicalPlan = parsePlan(sqlText)
}