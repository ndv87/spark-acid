package com.qubole.spark.hiveacid

import com.Environment

import java.sql.Timestamp.{valueOf => ts}

class HiveAcidTest extends Environment {

  import spkImpl._

  test("read acid table") {

    spark.conf.set("acid_max_num_buckets", 1)

    spark.sql("create table ice_db.trans (id int) stored as orc tblproperties('transactional'='true')")

    spark.sql("show create table ice_db.trans").show

    spark.sql("insert into ice_db.trans values (0),(1),(2),(3),(4)")
    spark.sql("update ice_db.trans set id = 2 where id = 1 or id = 0")

    spark.sql("select * from ice_db.trans").show

    spark.sql("create table ice_db.trans_part (id int) partitioned by (part string) stored as orc tblproperties('transactional'='true')")

    spark.sql("insert into ice_db.trans_part values (0, 'a'),(1, 'a'),(2, 'b'),(3, 'b'),(4, 'b')")
    spark.sql("update ice_db.trans_part set id = 2 where id = 1 or id = 0")

    spark.table("ice_db.trans_part").show


    val df = Seq(ts("2024-01-01 00:00:00")).toDF("tss")

    df.writeTo("ice_db.tbl_tss")
      .tableProperty("write.format.default", "orc")
      .tableProperty("write.orc.compression-codec", "zstd")
      .tableProperty("engine.hive.enabled", "true")
      .tableProperty("write.spark.accept-any-schema", "true")
      .using("iceberg").createOrReplace()

    spark.sql(s"update ice_db.tbl_tss set tss = timestamp '2025-01-01 00:00:00'").collect()
    val iceRow = spark.table("ice_db.tbl_tss").head().getTimestamp(0).toString
    assert(iceRow == "2025-01-01 00:00:00.0")

    spark.sql("create table ice_db.ins_only (id int) stored as orc tblproperties('transactional'='true', 'transactional_properties'='insert_only')")

    spark.sql("insert into ice_db.ins_only values (1),(2),(3),(4)")

    spark.table("ice_db.ins_only").show


    spark.sql("insert into ice_db.trans values (5)")
    spark.sql("delete from ice_db.trans where id = 2")

    spark.table("ice_db.trans").show

    spark.sql("create table ice_db.trans_target (id int) stored as orc tblproperties('transactional'='true')")


    spark.sql(
      """merge into ice_db.trans_target as t using ice_db.trans as s on s.id=t.id
        | WHEN MATCHED THEN UPDATE SET t.id=s.id
        | WHEN NOT MATCHED THEN INSERT *
        |""".stripMargin)
    spark.table("ice_db.trans_target").show

    spark.sql(
      """merge into ice_db.trans_target as t using ice_db.trans as s on s.id=t.id
        | WHEN MATCHED AND s.id = 5 THEN DELETE
        | WHEN MATCHED THEN UPDATE SET t.id=s.id
        | WHEN NOT MATCHED THEN INSERT *
        |""".stripMargin)
    spark.table("ice_db.trans_target").show
  }
}