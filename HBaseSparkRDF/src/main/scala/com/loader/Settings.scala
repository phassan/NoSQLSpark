package com.loader

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.io.Source
import java.io.{ FileNotFoundException, IOException }

object Settings {

  val sparkConf = loadSparkConf()
  val sparkSession = getSparkSession()

  def loadSparkConf(): SparkConf = {

    val conf = new SparkConf()
      .setAppName("HBaseSpark-Loader")
      .setMaster("local[*]")

    conf
  }

  def getSparkSession(): SparkSession = {

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    spark
  }

  var inputRDFDataset = ""
  var prefixFile = ""
  var table = ""

  def userSettings(_rdfFile: String, _prefixFile: String, _table: String) = {

    this.inputRDFDataset = _rdfFile
    this.prefixFile = _prefixFile
    this.table = _table

  }

}