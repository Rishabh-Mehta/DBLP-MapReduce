package com.dblp.mapreduce.publication_venue

import java.io.ByteArrayInputStream
import java.lang

import com.dblp.mapreduce.utils.Utils
import javax.xml.stream.XMLInputFactory
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.apache.log4j.BasicConfigurator
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.control.Breaks.break

/**
 * Task 4    List of all publications that contain the highest number of authors at each venue.
 * Mapper  : Maps ((Publication,Venue),Author_Count)
 * Reducer : Receives the mapper input and here we check every Reducer input and insert it into a TreeMap[String, String]
 *           with Venue as key and (Author_Count,Publication) as value.For every Reducer input we check if that venue is
 *           added to TreeMap if not we add it.If it is already added and Author_Count is equal then we append the current
 *           publication to the value.If it is less than current Author_Count then we rewrite the value with current Author_Count
 *           and Publication.
 * Cleanup : This receives the TreeMap and here we write to context
 *           Venue :  (((Venue,Publication),Author_Count))
 */


object Publication_Venue_HighestAuthor {


  BasicConfigurator.configure()
  val logger: Logger = LoggerFactory.getLogger(Publication_Venue_HighestAuthor.getClass)


  class Map extends Mapper[LongWritable, Text, Text, IntWritable] {
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {

      try {
        val fileDtd = getClass.getClassLoader.getResource("dblp.dtd").toURI
        val inputXml =
          s"""<?xml version="1.0" encoding="ISO-8859-1"?>
      <!DOCTYPE dblp SYSTEM "$fileDtd">
      <dblp>""" + value.toString + "</dblp>"

        val preprocessedXML = xml.XML.loadString(inputXml)
        val authors = (preprocessedXML \\ "author").map(author => author.text.toLowerCase.trim).toList.sorted
        val publication = (preprocessedXML \\ "title").text.toString

        val authorCount = authors.size
        var document = value.toString
        document = document.replaceAll("\n", "").replaceAll("&", "&amp;").replaceAll("'", "&apos;").replaceAll("^(.+)(<)([^>/a-zA-z_]{1}[^>]*)(>)(.+)$", "$1&lt;$3&gt;$5")
        val reader = XMLInputFactory.newInstance.createXMLStreamReader(new ByteArrayInputStream(document.getBytes()))
        val venueMap = Utils.getVenueMap()
        var firsttag = true
        var xmlElement = ""
        var venue = ""
        logger.info("Starting to read venue")
        while (reader.hasNext) {
          try {
            reader.next
            if (reader.isStartElement) {
              if (firsttag) {
                xmlElement = reader.getLocalName
                if (venueMap.exists(vMap => vMap._1 == xmlElement)) {
                  venue = venueMap.get(xmlElement).get

                }
                firsttag = false
              }

            }
          }
          catch {
            case e: Exception =>
              logger.error("Error parsing XML", e)
          }
        }
        reader.close()
        if (authorCount > 0 && venue != "" && publication != "") {
          val output = new IntWritable(authorCount)
          context.write(new Text(publication + ":" + venue), output)
          logger.info("Mapper Output "+publication+":"+venue+" "+output)
        }
      }
      catch {
        case e: Exception =>
          logger.error("Error in parsing XML", e)
          throw new Exception(e)
      }
    }
  }

  class Reduce extends Reducer[Text, IntWritable, Text, IntWritable] {

    var author_count_publication = new mutable.TreeMap[String, String]()

    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = 0
      val scalaValues = values.asScala
      scalaValues.foreach(values => sum += values.get)
      var mapval = ""
      var old = ""
      var output = ""
      val keys = key.toString.split(":")
      val publication = keys(0)
      val venue = keys(1).toString
      val authorcount = sum
      if (author_count_publication.contains(venue)) {
        mapval = author_count_publication(venue)
        old = mapval.toString.split(":")(0)
        if (old.toInt < authorcount) {
          output = authorcount.toString + ":" + publication
          author_count_publication.update(venue, output)
        }
        else if (old.toInt == authorcount) {
          output = mapval.appendedAll(":" + publication)
          author_count_publication.update(venue, mapval + ":" + publication)
        }
      }
      else {
        author_count_publication.put(venue, authorcount.toString + ":" + publication)
      }
    }

    override def cleanup(context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      logger.info("Reducer Output "+author_count_publication)
      author_count_publication.foreach(entry => context.write(new Text(entry._1 + "->" + entry._2.split(":")(1)), new IntWritable(entry._2.split(":")(0).toInt)))
    }


  }

}
