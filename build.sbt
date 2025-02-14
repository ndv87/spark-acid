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

name := s"spark-acid-${sparkVersion.value}"

organization := "com.qubole"

/** *****************
 * Scala settings
 */

crossScalaVersions := Seq("2.13.8")

scalaVersion := crossScalaVersions.value.head

resolvers += Resolver.jcenterRepo
resolvers += "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
resolvers += "Atlassian's Maven Public Repository" at "https://packages.atlassian.com/maven-public/"
resolvers += "Maven Central Server" at "https://repo1.maven.org/maven2/"

/** ************************
 * Spark package settings
 */
sparkVersion := sys.props.getOrElse("spark.version", "3.3.2")

spIncludeMaven := true

spIgnoreProvided := true


dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.0",


  "org.apache.hive" % "hive-shims" % "3.1.0" % "test",
  "org.apache.hive" % "hive-storage-api" % "2.8.1" % "test",
  "org.apache.hive" % "hive-common" % "3.1.0" % "test",
  "org.apache.hive" % "hive-serde" % "3.1.0" % "test",
  "org.apache.hive" % "hive-exec" % "3.1.0" % "test",
  "org.apache.hive" % "hive-metastore" % "3.1.0" % "test",
  //  "org.apache.hive" % "hive-metastore" % "2.3.9" % "test",
  //  "org.apache.hive" % "hive-exec" % "2.3.9" % "test",
  "org.apache.hadoop" % "hadoop-client-api" % "3.2.4" % "provided",

  "org.apache.hive" % "hive-llap-client" % "3.1.0" % "test",
  "org.apache.hive" % "hive-llap-common" % "3.1.0" % "test",
  "org.apache.calcite" % "calcite-core" % "1.37.0",
  "org.codehaus.janino" % "janino" % "3.0.16" % "test",
  "org.codehaus.janino" % "commons-compiler" % "3.0.16" % "test"

).toSet

/** **********************
 * Library Dependencies
 */
libraryDependencies += "org.scala-lang" % "scala-library" % scalaVersion.value
libraryDependencies += "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.6"

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion.value % Test
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % Test
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion.value % Test
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % Test

libraryDependencies ++= Seq(
  // Adding test classifier seems to break transitive resolution of the core dependencies
  "org.apache.spark" %% "spark-hive" % sparkVersion.value % "provided" excludeAll(
    ExclusionRule("org.apache", "hadoop-common"),
    ExclusionRule("org.apache", "hadoop-hdfs")),
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided" excludeAll(
    ExclusionRule("org.apache", "hadoop-common"),
    ExclusionRule("org.apache", "hadoop-hdfs")),
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided" excludeAll(
    ExclusionRule("org.apache", "hadoop-common"),
    ExclusionRule("org.apache", "hadoop-hdfs")),
  "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "provided" excludeAll(
    ExclusionRule("org.apache", "hadoop-common"),
    ExclusionRule("org.apache", "hadoop-hdfs")),
  "org.apache.hadoop" % "hadoop-common" % "3.2.4" % "provided",
  "org.apache.hadoop" % "hadoop-client-api" % "3.2.4" % "provided",

  "org.apache.hadoop" % "hadoop-hdfs" % "3.2.4" % "provided",
  "org.apache.commons" % "commons-lang3" % "3.3.5" % "provided",
  // antlr-runtime
  "org.antlr" % "antlr4-runtime" % "4.7.2" % "provided",
  "org.apache.orc" % "orc-mapreduce" % "1.7.8" % "provided",
  "org.apache.orc" % "orc-core" % "1.7.8" % "provided"

)

lazy val scalatest = "org.scalatest" %% "scalatest" % "3.2.17"



// Dependencies for Test
libraryDependencies ++= Seq(
  "org.apache.iceberg" % "iceberg-hive-runtime" % "1.5.0"% "test",
  "org.apache.iceberg" % "iceberg-spark-runtime-3.3_2.13" % "1.5.0"% "test",
  "org.apache.iceberg" % "iceberg-core" % "1.5.0"% "test",
  "org.apache.iceberg" % "iceberg-hive-metastore" % "1.5.0"% "test",
  "org.apache.iceberg" % "iceberg-spark" % "1.5.0"% "test",
  "org.apache.iceberg" % "iceberg-common" % "1.5.0",
  "org.apache.hadoop" % "hadoop-common" % "3.2.4" % "provided",
  "org.apache.hadoop" % "hadoop-hdfs" % "3.2.4" % "provided",
  "org.apache.commons" % "commons-lang3" % "3.3.5" % "provided",
  // Dependencies for tests
  //
  "org.scalatest" %% "scalatest" % "3.2.17" % "test",
  "junit" % "junit" % "4.12" % "it,test",
  "com.novocode" % "junit-interface" % "0.11" % "it,test",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "test",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test",
  "org.apache.spark" %% "spark-hive" % sparkVersion.value % "test",
  "org.apache.calcite" % "calcite-core" % "1.37.0",
  "org.codehaus.janino" % "janino" % "3.0.16" % "test",
  "org.codehaus.janino" % "commons-compiler" % "3.0.16" % "test",

  "org.apache.hive" % "hive-shims" % "3.1.0" % "test",
  "org.apache.hive" % "hive-storage-api" % "2.8.1" % "test",
  "org.apache.hive" % "hive-common" % "3.1.0" % "test",
  "org.apache.hive" % "hive-serde" % "3.1.0" % "test",
//  "org.apache.hive" % "hive-exec" % "3.1.0" % "test",
  "org.apache.hive" % "hive-metastore" % "3.1.0" % "test",

  // https://mvnrepository.com/artifact/com.dimafeng/testcontainers-scala
  "com.dimafeng" %% "testcontainers-scala" % "0.41.4" % Test,
  "com.dimafeng" %% "testcontainers-scala-postgresql" % "0.41.4" % Test,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.41.4" % Test
)

// Shaded jar dependency
libraryDependencies ++= Seq(
  // intransitive() because we don't want to include any transitive dependencies of shaded-dependencies jar in main jar
  // ideally all such dependencies should be shaded inside shaded-dependencies jar
  "com.qubole" %% "spark-acid-shaded-dependencies" % sys.props.getOrElse("package.version", "0.1") intransitive()
)

/** ************************************
 * Remove Shaded Depenedency from POM
 */

import sbt.Keys.libraryDependencies
import sbt.Resolver

import java.util.Properties
import scala.xml.{Node => XmlNode, NodeSeq => XmlNodeSeq, _}
import scala.xml.transform.{RewriteRule, RuleTransformer}

pomPostProcess := { (node: XmlNode) =>
  new RuleTransformer(new RewriteRule {
    override def transform(node: XmlNode): XmlNodeSeq = node match {
      case e: Elem if e.label == "dependency" && e.child.filter(_.label == "groupId").text.mkString == "com.qubole" =>
        val organization = e.child.filter(_.label == "groupId").flatMap(_.text).mkString
        val artifact = e.child.filter(_.label == "artifactId").flatMap(_.text).mkString
        val version = e.child.filter(_.label == "version").flatMap(_.text).mkString
        Comment(s"dependency $organization#$artifact;$version has been omitted")
      case _ => node
    }
  }).transform(node).head
}

excludeDependencies ++= Seq(
  // hive
//  "org.apache.hive" % "hive-exec",
//  "org.apache.hive" % "hive-metastore",
//  "org.apache.hive" % "hive-jdbc",
//  "org.apache.hive" % "hive-service",
//  "org.apache.hive" % "hive-serde",
//  "org.apache.hive" % "hive-common",

  // orc
//  "org.apache.orc" % "orc-core",
//  "org.apache.orc" % "orc-mapreduce",

  "org.slf4j" % "slf4j-api"
)

// do not run test at assembly
test in assembly := {}

// Spark Package Section
spName := "qubole/spark-acid"

spShade := true

spAppendScalaVersion := true

publishMavenStyle := true

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)

val isNexus = sys.props.get("isNexus").getOrElse("false").toBoolean

import scala.io.Source

def loadNexusUrl(): String = {
  val props = new Properties()
  val source = Source.fromFile(Path.userHome / ".ivy2" / "nexus-url.conf")
  try {
    props.load(source.bufferedReader())
    props.getProperty("nexus.url", "")
  } finally {
    source.close()
  }
}

publishTo := {
  if (isNexus) {
    val nexusUrl = loadNexusUrl()
    Some("Nexus" at nexusUrl)
  } else {
    val githubUrl = "https://maven.pkg.github.com/ndv87/spark-acid"
    Some("GitHub" at githubUrl)
  }
 }

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentialsnexus")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

pomExtra :=
  <url>https://github.com/qubole/spark-acid</url>
    <scm>
      <url>git@github.com:qubole/spark-acid.git</url>
      <connection>scm:git:git@github.com:qubole/spark-acid.git</connection>
    </scm>
    <developers>
      <developer>
        <id>amoghmargoor</id>
        <name>Amogh Margoor</name>
        <url>https://github.com/amoghmargoor</url>
      </developer>
      <developer>
        <id>citrusraj</id>
        <name>Rajkumar Iyer</name>
        <url>https://github.com/citrusraj</url>
      </developer>
      <developer>
        <id>somani</id>
        <name>Abhishek Somani</name>
        <url>https://github.com/somani</url>
      </developer>
      <developer>
        <id>prakharjain09</id>
        <name>Prakhar Jain</name>
        <url>https://github.com/prakharjain09</url>
      </developer>
      <developer>
        <id>sourabh912</id>
        <name>Sourabh Goyal</name>
        <url>https://github.com/sourabh912</url>
      </developer>
    </developers>




import ReleaseTransformations._

// Add publishing to spark packages as another step.
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  pushChanges,
  releaseStepTask(spDist),
  releaseStepTask(spPublish)
)

/**
 * Antlr settings
 */
//antlr4Settings
//antlr4PackageName in Antlr4 := Some("com.qubole.spark.datasources.hiveacid.sql.catalyst.parser")
//antlr4GenListener in Antlr4 := true
//antlr4GenVisitor in Antlr4 := true
//antlr4Version := "4.9.2"

testOptions in Test += Tests.Argument("-DcontinueOnError=true")
/** *****************
 * Test settings
 */

parallelExecution in IntegrationTest := false

// do not run test at assembly
test in assembly := {}

// do not add scala in fat jar
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

//Integration test
lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    libraryDependencies += scalatest % "it"
  )

// exclude antlr classes from assembly since those
// are available in spark at runtime
// any other classes to be excluded from assembly
// should be added here
assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {
    _.data.getName.contains("antlr")
  }
}

/** *********************
 * Release settings
 */

publishMavenStyle := true


import ReleaseTransformations._
//
//// Add publishing to spark packages as another step.
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  pushChanges
)
