package com.loader

import scala.io.Source
import org.apache.spark.rdd.RDD
import java.io.{ FileNotFoundException, IOException }
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, HColumnDescriptor }
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{ Put, HTable }
import org.apache.hadoop.hbase.regionserver.BloomType

object TablePopulator {

  val spark = Settings.getSparkSession()
  import spark.implicits._
  type Data = (String, String, String)
  private val _dataset = Settings.inputRDFDataset
  var prefixes = scala.collection.mutable.Map[String, String]()

  def getPrefixes(): org.apache.spark.broadcast.Broadcast[scala.collection.mutable.Map[String, String]] = {
    try {
      for (line <- Source.fromFile(Settings.prefixFile).getLines) {
        val arr = line.split("&&")
        prefixes += (arr(1).trim() -> arr(0).trim())
      }
    } catch {
      case e: FileNotFoundException => println("Couldn't find that prefix file.")
      case e: IOException           => println("Got an IOException!")
    }

    val _prefixes = spark.sparkContext.broadcast(prefixes)
    _prefixes
  }

  def loadTable() = {

    val _prefixes = getPrefixes()
    val conf = HBaseConfiguration.create()
    val tableName = Settings.table

    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val admin = new HBaseAdmin(conf)

    if (admin.isTableAvailable(tableName)) {
      println("Table " + tableName + " exist!")
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
      println("Table " + tableName + " disabled and deleted.")
      print("Creating table: " + tableName + "\t")
      val tableDesc = new HTableDescriptor(tableName)
      val columnDesc = new HColumnDescriptor("cf".getBytes()).setBloomFilterType(BloomType.ROWCOL).setMaxVersions(200)
      tableDesc.addFamily(columnDesc)
      admin.createTable(tableDesc)

    } else {
      print("Creating table: " + tableName + "\t")
      val tableDesc = new HTableDescriptor(tableName)
      val columnDesc = new HColumnDescriptor("cf".getBytes()).setBloomFilterType(BloomType.ROWCOL).setMaxVersions(200)
      tableDesc.addFamily(columnDesc)
      admin.createTable(tableDesc)
    }

    val table = new HTable(conf, tableName)
    val rdfRDD = spark.sparkContext.textFile(_dataset)
    val parsedData = rdfRDD.flatMap(l => Helper.parseDataset(l, _prefixes))
    val rdfGroupBy = parsedData.map(x => (x._1, (x._2, x._3))).groupByKey().collectAsMap()

    rdfGroupBy.foreach(elem => {
      var rowKey = elem._1

      elem._2.foreach(innerElem => {
        var col = innerElem._1
        var put = new Put(rowKey.getBytes())
        var value = innerElem._2
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(col), Bytes.toBytes(value))
        table.put(put)
      })
    })
    table.flushCommits()
  }
}