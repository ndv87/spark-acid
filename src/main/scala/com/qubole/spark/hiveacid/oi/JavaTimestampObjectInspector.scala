package com.qubole.spark.hiveacid.oi

import com.qubole.shaded.hadoop.hive.common.`type`.Timestamp
import com.qubole.shaded.hadoop.hive.serde2.io.TimestampWritableV2
import com.qubole.shaded.hadoop.hive.serde2.objectinspector.primitive.{AbstractPrimitiveJavaObjectInspector, SettableTimestampObjectInspector}
import com.qubole.shaded.hadoop.hive.serde2.typeinfo.TypeInfoFactory

class JavaTimestampObjectInspector extends AbstractPrimitiveJavaObjectInspector(TypeInfoFactory.timestampTypeInfo)
  with SettableTimestampObjectInspector {

  override def getPrimitiveWritableObject(o: AnyRef): TimestampWritableV2 = {
    if (o == null) null else new TimestampWritableV2(o.asInstanceOf[Timestamp])
  }

  override def getPrimitiveJavaObject(o: AnyRef): Timestamp = {
    if (o == null) null else o.asInstanceOf[Timestamp]
  }

  override def copyObject(o: AnyRef): AnyRef = {
    if (o == null) null else new Timestamp(o.asInstanceOf[Timestamp])
  }

  def get(o: AnyRef): Timestamp = o.asInstanceOf[Timestamp]

  @deprecated("Use the non-deprecated version of set", "Since some version")
  override def set(o: AnyRef, value: java.sql.Timestamp): AnyRef = {
    if (value == null) null else {
      o.asInstanceOf[Timestamp].setTimeInMillis(value.getTime, value.getNanos)
      o
    }
  }

  override def set(o: AnyRef, value: Timestamp): AnyRef = {
    if (value == null) null else {
      o.asInstanceOf[Timestamp].set(value)
      o
    }
  }

  override def set(o: AnyRef, bytes: Array[Byte], offset: Int): AnyRef = {
    TimestampWritableV2.setTimestamp(o.asInstanceOf[Timestamp], bytes, offset)
    o
  }

  override def set(o: AnyRef, tw: TimestampWritableV2): AnyRef = {
    if (tw == null) null else {
      val t = o.asInstanceOf[Timestamp]
      t.set(tw.getTimestamp)
      t
    }
  }

  @deprecated("Use the non-deprecated version of create", "Since some version")
  override def create(value: java.sql.Timestamp): AnyRef = {
    Timestamp.ofEpochMilli(value.getTime, value.getNanos)
  }

  override def create(value: Timestamp): AnyRef = {
    new Timestamp(value)
  }

  override def create(bytes: Array[Byte], offset: Int): AnyRef = {
    TimestampWritableV2.createTimestamp(bytes, offset)
  }
}