package com.loader

import scala.sys.process._

object Helper {

  def parseDataset(line: String, prefixes: org.apache.spark.broadcast.Broadcast[scala.collection.mutable.Map[String, String]]): Option[TablePopulator.Data] = {

    val fields = line.split("\\s+")
    if (fields.length > 2) {

      val _subject = getParsedField(fields(0), prefixes.value)

      val _predicate = getParsedField(fields(1), prefixes.value)

      val _object = getParsedField(fields.drop(2).mkString(" ").stripSuffix(".").trim, prefixes.value)

      return Some(_subject, _predicate, _object)
    } else {
      return None
    }
  }

  def replaceNamespace(field: String, prefixes: scala.collection.mutable.Map[String, String]): String = {
    var _field = ""

    if (isURI(field)) {
      prefixes foreach {
        case (key, value) =>
          if (field.contains(key)) {
            _field = field.replace(key, prefixes(key))
            //            .replace("<", "").replace(">", "")
            return _field
          }
      }
      _field = field
      //      .replace("<", "").replace(">", "")
      return _field
    } else {
      _field = field
      return _field
    }

  }

  def isURI(field: String): Boolean = {
    if (field.contains("<") && field.endsWith(">")) {
      return true
    } else {
      return false
    }
  }

  def replaceTypeExtn(field: String): String = {

    val _str = field.replace("^^xsd:date", "")
      .replace("^^bsbm:USD", "")
      .replace("^^xsd:integer", "")
      .replace("^^xsd:string", "")
      .replace("\"", "")
      .replace("T00:00:00Time", "")

    _str
  }

  def getParsedField(field: String, prefixes: scala.collection.mutable.Map[String, String]): String = {

    val _str1 = replaceNamespace(field, prefixes)
    var _str2 = replaceTypeExtn(_str1)

    if (_str2.endsWith("/")) {
      _str2 = _str2.substring(0, _str2.length() - 1).trim()
    }

    _str2
  }

  def getPartitionName(v: String): String = {
    v.replaceAll("[:|/|\\.]", "_")
  }

}