package com

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.io._
import scala.collection.Seq
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

trait Environment extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach with BeforeAfterForSpark  {

  def createDB(name: String) = {
    val query = s"create database if not exists $name location '$absolutePathSparkWarehouseDirNoDisk'"
    println(query)
    spark.sql(query)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createDB("ice_db")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  def toUnixAbsPath(filePath: String): String = {
    (if (filePath.head.isLetter || filePath.head == '/')
      ""
    else
      new File(".").getCanonicalPath.replaceAll("\\\\", "/") + "/") +
      filePath.replaceAll("\\\\", "/")
  }

  def getRelativeFilePathFromTestResources(fileName: String): String = {
    Seq("src", "test", "resources", fileName).mkString(sep).replaceAll("\\\\", "/")
  }

  def getAbsFilePathFromTestResources(fileName: String): String = {
    toUnixAbsPath(new File(".").getCanonicalPath + Seq("src", "test", "resources", fileName).mkString(sep, sep, ""))
  }

  def getAbsFilePathCompilerTemplate(templateName: String): String = {
    toUnixAbsPath(new File(".").getCanonicalPath + Seq("compiler", "templates", templateName).mkString(sep, sep, ""))
  }


  def writeFile(filename: String, lines: String): Unit = {
    new File(filename.split("/").init.mkString("/")).mkdirs()

    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(lines)
    bw.write("\n")
    bw.close()
  }

  def writeFile(filename: String, lines: Seq[String]): Unit = {
    new File(filename.split("/").init.mkString("/")).mkdirs()

    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- lines) {
      bw.write(line)
      bw.write("\n")
    }
    bw.close()
  }


  def awaitInf[T](future: Future[T]) = Await.result(future, Duration.Inf)

}




