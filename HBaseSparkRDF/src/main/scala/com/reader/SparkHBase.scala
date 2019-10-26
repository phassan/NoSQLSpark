package com.reader

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{ SparkConf, SparkContext }
import java.util.NavigableMap
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j._

object SparkHBase extends Serializable {

  case class Row(s: String, rdf_type: Option[Array[String]], ub_teacherOf: Option[Array[String]],
                 ub_takesCourse: Option[Array[String]], ub_publicationAuthor: Option[Array[String]],
                 ub_advisor: Option[String], ub_doctoralDegreeFrom: Option[String], ub_emailAddress: Option[String],
                 ub_headOf: Option[String], ub_mastersDegreeFrom: Option[String], ub_memberOf: Option[String],
                 ub_name: Option[String], ub_researchInterest: Option[String], ub_subOrganizationOf: Option[String],
                 ub_teachingAssistantOf: Option[String], ub_telephone: Option[String],
                 ub_undergraduateDegreeFrom: Option[String], ub_worksFor: Option[String])
  org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)

  object Row extends Serializable {
    def parseRow(result: Result, tableName: String, quorum: String): Row = {

      val p0 = Bytes.toString(result.getRow())

      val t = tableName
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", quorum)
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      val table = new HTable(conf, t)
      val key = new Get(result.getRow())
      key.setMaxVersions()
      val row = table.get(key)
      val content: NavigableMap[Array[Byte], NavigableMap[Array[Byte], NavigableMap[java.lang.Long, Array[Byte]]]] = row.getMap

      val a1 = result.getValue(cfDataBytes, Bytes.toBytes("<rdf:type>"))
      val typeArray = new ArrayBuffer[String]

      if (a1 != null) {
        for (entry <- content.entrySet) {
          for (sub_entry <- entry.getValue.entrySet) {
            for (sub_entry2 <- sub_entry.getValue.entrySet) {
              val col = Bytes.toString(sub_entry.getKey)
              if (col == "<rdf:type>") {
                typeArray += Bytes.toString(sub_entry2.getValue)
              }
            }
          }
        }
      }
      val p1 = if (!typeArray.isEmpty) Some(typeArray.toArray) else null

      val a2 = result.getValue(cfDataBytes, Bytes.toBytes("<ub:teacherOf>"))
      val teacherOfArray = new ArrayBuffer[String]
      if (a2 != null) {
        for (entry <- content.entrySet) {
          for (sub_entry <- entry.getValue.entrySet) {
            for (sub_entry2 <- sub_entry.getValue.entrySet) {
              val col = Bytes.toString(sub_entry.getKey)
              if (col == "<ub:teacherOf>") {
                teacherOfArray += Bytes.toString(sub_entry2.getValue)
              }
            }
          }
        }
      }
      val p2 = if (!teacherOfArray.isEmpty) Some(teacherOfArray.toArray) else null

      val a3 = result.getValue(cfDataBytes, Bytes.toBytes("<ub:takesCourse>"))
      val takesCourseArray = new ArrayBuffer[String]
      if (a3 != null) {
        for (entry <- content.entrySet) {
          for (sub_entry <- entry.getValue.entrySet) {
            for (sub_entry2 <- sub_entry.getValue.entrySet) {
              val col = Bytes.toString(sub_entry.getKey)
              if (col == "<ub:takesCourse>") {
                takesCourseArray += Bytes.toString(sub_entry2.getValue)
              }
            }
          }
        }
      }
      val p3 = if (!takesCourseArray.isEmpty) Some(takesCourseArray.toArray) else null

      val a4 = result.getValue(cfDataBytes, Bytes.toBytes("<ub:publicationAuthor>"))
      val publicationAuthorArray = new ArrayBuffer[String]
      if (a4 != null) {
        for (entry <- content.entrySet) {
          for (sub_entry <- entry.getValue.entrySet) {
            for (sub_entry2 <- sub_entry.getValue.entrySet) {
              val col = Bytes.toString(sub_entry.getKey)
              if (col == "<ub:publicationAuthor>") {
                publicationAuthorArray += Bytes.toString(sub_entry2.getValue)
              }
            }
          }
        }
      }
      val p4 = if (!publicationAuthorArray.isEmpty) Some(publicationAuthorArray.toArray) else null

      val a5 = result.getValue(cfDataBytes, Bytes.toBytes("<ub:advisor>"))
      val p5 = if (a5 != null) Some(Bytes.toString(a5)) else null

      val a6 = result.getValue(cfDataBytes, Bytes.toBytes("<ub:doctoralDegreeFrom>"))
      val p6 = if (a6 != null) Some(Bytes.toString(a6)) else null

      val a7 = result.getValue(cfDataBytes, Bytes.toBytes("<ub:emailAddress>"))
      val p7 = if (a7 != null) Some(Bytes.toString(a7)) else null

      val a8 = result.getValue(cfDataBytes, Bytes.toBytes("<ub:headOf>"))
      val p8 = if (a8 != null) Some(Bytes.toString(a8)) else null

      val a9 = result.getValue(cfDataBytes, Bytes.toBytes("<ub:mastersDegreeFrom>"))
      val p9 = if (a9 != null) Some(Bytes.toString(a9)) else null

      val a10 = result.getValue(cfDataBytes, Bytes.toBytes("<ub:memberOf>"))
      val p10 = if (a10 != null) Some(Bytes.toString(a10)) else null

      val a11 = result.getValue(cfDataBytes, Bytes.toBytes("<ub:name>"))
      val p11 = if (a11 != null) Some(Bytes.toString(a11)) else null

      val a12 = result.getValue(cfDataBytes, Bytes.toBytes("<ub:researchInterest>"))
      val p12 = if (a12 != null) Some(Bytes.toString(a12)) else null

      val a13 = result.getValue(cfDataBytes, Bytes.toBytes("<ub:subOrganizationOf>"))
      val p13 = if (a13 != null) Some(Bytes.toString(a13)) else null

      val a14 = result.getValue(cfDataBytes, Bytes.toBytes("<ub:teachingAssistantOf>"))
      val p14 = if (a14 != null) Some(Bytes.toString(a14)) else null

      val a15 = result.getValue(cfDataBytes, Bytes.toBytes("<ub:telephone>"))
      val p15 = if (a15 != null) Some(Bytes.toString(a15)) else null

      val a16 = result.getValue(cfDataBytes, Bytes.toBytes("<ub:undergraduateDegreeFrom>"))
      val p16 = if (a16 != null) Some(Bytes.toString(a16)) else null

      val a17 = result.getValue(cfDataBytes, Bytes.toBytes("<ub:worksFor>"))
      val p17 = if (a17 != null) Some(Bytes.toString(a17)) else null

      Row(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17)
    }
  }

  final val cfData = "cf"
  final val cfDataBytes = Bytes.toBytes(cfData)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val config = new SparkConf()
    config.setAppName("HBaseSpark-Reader")
      .setMaster("local[*]")
    val sc = new SparkContext(config)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val table = args(0) // "lubm2"  
    val quorum = args(1) // "10.0.1.1,10.0.1.2,10.0.1.3" 
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", quorum)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, table)

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val rdd = hBaseRDD.map(tuple => tuple._2)

    val resultRDD = rdd.map(result => Row.parseRow(result, table, quorum))
    val resultDF = resultRDD.toDF()
    val dataFrameET = System.currentTimeMillis()

    resultDF.createOrReplaceTempView("table")

    val queryST = System.currentTimeMillis()
    val result = sqlContext.sql(s"""SELECT s FROM table
                                    LATERAL VIEW EXPLODE(rdf_type) EXPLODED_NAMES AS rdf_type_lve
                                    LATERAL VIEW EXPLODE(ub_takesCourse) EXPLODED_NAMES AS ub_takesCourse_lve
                                    WHERE rdf_type_lve = '<ub:GraduateStudent>' 
                                    AND ub_takesCourse_lve = '<http://www.Department0.University0.edu/GraduateCourse0>' """)

    // result.show()
    //    println(result.count())
    result.write.mode("overwrite").text(args(2))
    val queryET = System.currentTimeMillis()
    println("Query execution time:- " + (queryET - queryST) + " Millis, in Second:- " + (queryET - queryST) * 0.001 + " Sec")

    sc.stop()
  }

}