package com.reader

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object SparkCassandra {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val table = args(0)

    val conf = new SparkConf(true)
      .setAppName("CassandraSpark-Reader")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cassandra.connection.port", "9042")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val readDF = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> "database"))
      .load()
    readDF.createOrReplaceTempView("tt")

    val pList = List("<rdf:type>", "<ub:takesCourse>")
    pList.foreach(p => {
      var tableQuery = "SELECT s, o FROM tt WHERE p = '%s'".format(p)
      var df = spark.sql(tableQuery)
      var tableName = p.replace(":", "_").replaceAll("\\<", "").replaceAll("\\>", "")
      df.createOrReplaceTempView(tableName)
    })

    val queryST = System.currentTimeMillis()

    val result = spark.sql(s""" SELECT T0.s AS X FROM rdf_type AS T0 JOIN ub_takesCourse AS T1 ON (T0.s = T1.s)
    WHERE T0.o = '<ub:GraduateStudent>' AND T1.o = '<http://www.Department0.University0.edu/GraduateCourse0>' """)

    result.write.mode("overwrite").text(args(1))
    val queryET = System.currentTimeMillis()
    println("Query execution time:- " + (queryET - queryST) + " Millis, in Second:- " + (queryET - queryST) * 0.001 + " Sec")

    spark.stop()

  }

}
