package com.loader

import org.apache.log4j._

object Driver {

  type Data = (String, String, String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    Settings.userSettings(args(0), args(1), args(2))

    val dataLoadST = System.currentTimeMillis()

      TablePopulator.loadTable()
      val dataLoadET = System.currentTimeMillis()
      println("Data load time:- " + (dataLoadET - dataLoadST) + " Millis, in Second:- " + (dataLoadET - dataLoadST) * 0.001 + " Sec")
       
    Settings.sparkSession.stop()
  }
}