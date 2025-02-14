package com

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.GenericContainer.FileSystemBind
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.wait.strategy.Wait

import java.io.{File, FileDescriptor, FileOutputStream, PrintStream}
import java.nio.file.{Files, Paths}
import java.time.Duration
import scala.io.Source

object Utils {
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }
}

trait HMSContainer extends BeforeAndAfterAll {
  self: Suite with Environment =>

  lazy val HMSContainer = {
    val container = GenericContainer(dockerImage = "apache/hive:3.1.3",
      exposedPorts = List(9083),
      env = Map[String, String](
        "HIVE_CUSTOM_CONF_DIR" -> "/hive_custom_conf",
        "SERVICE_NAME" -> "metastore",
      ),
      fileSystemBind = List(
        FileSystemBind(s"${(absolutePathSparkWarehouseDir)}/docker_conf", "/hive_custom_conf", BindMode.READ_WRITE)),
      waitStrategy = Wait.forLogMessage(".*Starting Hive Metastore Server.*", 1)
    )

    container.underlyingUnsafeContainer.withCreateContainerCmdModifier(cmd => cmd.withUser("root"))
    container
  }


  override def afterAll(): Unit = {
    HMSContainer.stop()
    super.afterAll()
  }
}


trait HS2Container extends HMSContainer with BeforeAndAfterAll {
  self: Suite with BeforeAfterForSpark with Environment =>

  lazy val HS2Container = {
    HMSContainer.start()
    val container = GenericContainer(dockerImage = "apache/hive:3.1.3",
      exposedPorts = List(10000, 10002),
      env = Map[String, String](
        "HIVE_CUSTOM_CONF_DIR" -> "/hive_custom_conf",
        "SERVICE_OPTS" -> s"-Dhive.metastore.uris=thrift://${HMSContainer.container.getContainerInfo.getNetworkSettings.getIpAddress}:9083",
        "SERVICE_NAME" -> "hiveserver2",
        "IS_RESUME" -> "true"
      ),
      fileSystemBind = List(
        FileSystemBind(s"${(absolutePathSparkWarehouseDir)}", absolutePathSparkWarehouseDirNoDisk, BindMode.READ_WRITE),
        FileSystemBind(s"${(absolutePathSparkWarehouseDir)}/docker_conf", "/hive_custom_conf", BindMode.READ_WRITE)),
      //      waitStrategy = Wait.forLogMessage(".*Hive Session ID.*", 1)
      waitStrategy = Wait.forListeningPorts(10002).withStartupTimeout(Duration.ofMinutes(5))

    )

    container.underlyingUnsafeContainer.withCreateContainerCmdModifier(cmd => cmd.withUser("root"))
    container
  }


  def beeline(sql: String) = {
    val res = HS2Container.execInContainer("beeline", "-u", s"jdbc:hive2://localhost:10000", "--silent=true", "-e", s"$sql")
    if (res.getStderr.contains("Error")) throw new IllegalStateException(res.getStderr) else res.getStdout
  }

  override def afterAll(): Unit = {
    HS2Container.stop()
    super.afterAll()
  }
}

trait BeforeAfterForSpark extends BeforeAndAfterAll with BeforeAndAfterEach with HS2Container {
  self: Suite with Environment =>

  lazy val absolutePathSparkWarehouseDir: String =
    (new File(".").getCanonicalPath +
      Seq("target", "sparkwarehouse", this.getClass.getCanonicalName.toLowerCase).mkString(sep, sep, ""))
      .replaceAll("\\.", "-")
      .toLowerCase

  lazy val absolutePathSparkWarehouseDirNoDisk = absolutePathSparkWarehouseDir
    .replaceAll("^\\w:", "")
    .replaceAll(raw"\\", raw"/")


  lazy val absoluteHadoopHomeDir: String =
    new File(".").getCanonicalPath +
      Seq("src", "test", "resources", "hadoop").mkString(sep, sep, "")


  @transient var _sc: SparkContext = _
  @transient var _spark: SparkSession = _

  def sc: SparkContext = _sc

  def spark: SparkSession = _spark

  val numCores = "*"

  implicit lazy val spkImpl = spark.implicits

  val sep = File.separator

  System.setProperty("hadoop.home.dir", absoluteHadoopHomeDir)

  System.setErr(new SuppressErrors("org.apache.hadoop.fs"))

  Utils.deleteRecursively(new File(absolutePathSparkWarehouseDir))
  val hadoopConf = new Configuration()
  val hadoopFS = FileSystem.get(hadoopConf)

  val hiveConfTemplate = Source.fromFile(getAbsFilePathFromTestResources("docker_conf/hive-site.xml")).getLines().mkString("\n")

  val hiveConfNew = hiveConfTemplate.replace("{warehouse}", absolutePathSparkWarehouseDirNoDisk)
  val hiveConfDir = Paths.get(absolutePathSparkWarehouseDir + "/docker_conf")
  if (!Files.exists(hiveConfDir)) Files.createDirectories(hiveConfDir)
  val hiveConfNewPath = absolutePathSparkWarehouseDir + "/docker_conf/hive-site.xml"
  writeFile(hiveConfNewPath, hiveConfNew)


  HS2Container.start()


  val conf = new SparkConf().setMaster(s"local[$numCores]")
    .set("spark.sql.shuffle.partitions", "1")
    .set("hive.exec.scratchdir", s"$absolutePathSparkWarehouseDirNoDisk${sep}scratch")
    .set("hive.exec.dynamic.partition", "true")
    .set("hive.exec.dynamic.partition.mode", "nonstrict")
    .set("spark.sql.warehouse.dir", s"$absolutePathSparkWarehouseDirNoDisk")
    .set("spark.hadoop.hive.metastore.uris", s"thrift://${HMSContainer.host}:${HMSContainer.mappedPort(9083)}")
    //    .set("spark.sql.hive.metastore.version", "2.3.9")
    //    .set("spark.sql.hive.metastore.jars", "maven")
    .set("spark.sql.hive.metastore.version", "3.1.3")
    .set("spark.sql.hive.metastore.jars", "maven")

    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    .set("spark.kryo.registrationRequired", "true")

    .set("spark.sql.parquet.writeLegacyFormat", "true")
    .set("spark.driver.host", "127.0.0.1")
    .set("spark.sql.caseSensitive", "false")
    .set("spark.sql.hive.convertMetastoreParquet", "false")
    .set("spark.driver.allowMultipleContexts", "true")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    //
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .set("spark.sql.catalog.spark_catalog.type", "hive")
    .set("spark.sql.defaultCatalog", "spark_catalog")

    .set("spark.sql.catalog.spark_catalog.warehouse", s"$absolutePathSparkWarehouseDirNoDisk")

    .set("spark.sql.adaptive.enabled", "false")
    .set("spark.sql.storeAssignmentPolicy", "ANSI")

    .set("spark.io.compression.codec", "zstd")
  //    .set("spark.sql.session.timeZone", "UTC")

  override def beforeAll(): Unit = {
    super.beforeAll()
    getOrCreateSparkSession()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    getOrCreateSparkSession()
  }

  def getOrCreateSparkSession() = {
    if (_spark == null) {
      println("Create new Spark Context")
      println(s"absolutePathSparkWarehouseDir: $absolutePathSparkWarehouseDir")
      println(s"absolutePathSparkWarehouseDirNoDisk: $absolutePathSparkWarehouseDirNoDisk")
      println(s"absoluteHadoopHomeDir: $absoluteHadoopHomeDir")

      _spark = SparkSession.builder()
        .config(conf)
        .enableHiveSupport()
        .appName("Test")
        .getOrCreate()
      _sc = _spark.sparkContext

      Files.createDirectories(Paths.get(absolutePathSparkWarehouseDir))

    } else {
      println("Get already created Spark Context")
    }
  }



}

class SuppressErrors(packages: String*) extends PrintStream(new FileOutputStream(FileDescriptor.err)) {

  def filter(): Boolean =
    Thread.currentThread()
      .getStackTrace
      .exists(el => packages.exists(el.getClassName.contains))

  override def write(b: Int): Unit = {
    if (!filter()) super.write(b)
  }

  override def write(buf: Array[Byte], off: Int, len: Int): Unit = {
    if (!filter()) super.write(buf, off, len)
  }

  override def write(b: Array[Byte]): Unit = {
    if (!filter()) super.write(b)
  }
}

