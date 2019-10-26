package com.loader

import scala.io.Source
import org.apache.spark.rdd.RDD
import java.io.{ FileNotFoundException, IOException }
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import scala.collection.mutable.ListBuffer

object TablePopulator {

  val spark = Settings.getSparkSession()
  import spark.implicits._
  type Data = (String, String, String)
  case class triple(subject: String, predicate: String, value: String)

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
    val rdfRDD = spark.sparkContext.textFile(_dataset)
    val parsedDF = rdfRDD.flatMap(l => Helper.parseDataset(l, _prefixes)).toDF("s", "p", "o")
    val tableName = Settings.table

    parsedDF.createOrReplaceTempView("t")
    val uPredicateArr = spark.sql(s""" SELECT DISTINCT p FROM t """).select($"p")
      .map(r => r.getString(0))
      .collect

    //    parsedDF.show()

    CassandraConnector(Settings.sparkConf).withSessionDo { session =>
      session.execute(
        s"""CREATE KEYSPACE IF NOT EXISTS database WITH
            | replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }""".stripMargin)
      session.execute(s"""DROP TABLE IF EXISTS database.$tableName""")
      session.execute(s"""CREATE TABLE IF NOT EXISTS database.$tableName (s text, p text, o text, PRIMARY KEY ((s, o), p))""")
    }

    parsedDF.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableName, "keyspace" -> "database"))
      .mode(org.apache.spark.sql.SaveMode.Append)
      .save()

  }
}