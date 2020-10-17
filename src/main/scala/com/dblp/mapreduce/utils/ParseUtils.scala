package com.dblp.mapreduce.utils


import javax.xml.parsers.SAXParserFactory

import scala.xml.{Elem, XML}

object ParseUtils {
  val xmlParser = SAXParserFactory.newDefaultInstance().newSAXParser()
  private val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI
  def formatXml(document :String):Elem ={
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dtdFilePath"><dblp>$document</dblp>"""

    XML
      .withSAXParser(xmlParser)
      .loadString(xmlString)
  }

}
