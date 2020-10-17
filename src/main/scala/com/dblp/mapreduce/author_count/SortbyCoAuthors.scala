package com.dblp.mapreduce.author_count

import javax.xml.parsers.SAXParserFactory
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import com.dblp.mapreduce.utils.ApplicationConstants
import com.typesafe.config.{Config, ConfigFactory}

import org.slf4j.{Logger, LoggerFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import javax.xml.parsers.SAXParserFactory
import java.io.{ByteArrayInputStream, IOException}

import scala.collection.mutable.ArrayBuffer
import scala.xml.{Elem, XML}

class SortbyCoAuthors  {


  class Map extends Mapper[LongWritable,Text,Text,IntWritable] {
    private val xmlParser = SAXParserFactory.newInstance().newSAXParser()
    private val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI


//    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
//
//      val publicationXML = getPublicationXML(value.toString)
//      val authors = getCoAuthors(publicationXML)
//
//      // map an author to a co-author
//      if (authors.nonEmpty) {
//        authors.foreach { author =>
//          authors.foreach {
//            authorRecursive =>
//              if (author != authorRecursive) {
//                logger.info("Mapper SortByNumCoAuthorsMapper emitting (key, value) pair : " + "(" + author + "," + authorRecursive + ")")
//                context.write(new Text(author), new Text(authorRecursive))
//              }
//          }
//        }
//      }
//    }


    def getPublicationXML(publicationText: String): Elem = {
      val xmlString =
        s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dtdFilePath"><dblp>$publicationText</dblp>"""

      XML
        .withSAXParser(xmlParser)
        .loadString(xmlString)

    }
    def getCoAuthors(publicationElement: Elem): ArrayBuffer[String] = {
      val authors = new ArrayBuffer[String]()

      var author = ""

      publicationElement.child.head.label match {

        case "book" =>
          author = "editor"
        case "proceedings" =>
          author = "editor"
        case _ =>
          author = "author"
      }

      (publicationElement \\ author).foreach { node =>
        if (node.text != null) {
          authors += node.text
        }
      }
      authors
    }
}





}
