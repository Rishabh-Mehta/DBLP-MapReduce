package com.dblp.mapreduce.author_publication

import java.io.ByteArrayInputStream
import java.lang

import com.dblp.mapreduce.utils.Utils
import javax.xml.stream.XMLInputFactory
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.apache.log4j.BasicConfigurator
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.control.Breaks.break


/**
 * Task 1    Compute top 10 published authors at each venue.
 * Mapper  : Maps ((Author,Venue),Publication_Count) for each publication
 * Reducer : Receives the mapper input from multiple publications and combines multiple inputs with same key and puts
 *           them in a HashMap[String, Integer] with key (Author,Venue) and value total of Publication_Count
 * Cleanup : This receives the HashMap and here we group by Venue which results in a HashMap with key being venue and its
 *           value a HashMap of ((Author,Venue),Author_Count) which is converted to a sequence and sorted on Author_Count
 *           then top 10 records are taken,Which gives us the output for each venue.
 *           Venue :  (((Author,Venue),Publication_Count),((Author,Venue),Publication_Count),...) Top 10
 */

object Top_Authors_Published_Venue {
  BasicConfigurator.configure()
  val logger = LoggerFactory.getLogger(Top_Authors_Published_Venue.getClass)


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

        val publication = (preprocessedXML \\ "@title").toString()
        val authorCount = authors.size
        var document = value.toString
        document = document.replaceAll("\n", "").replaceAll("&", "&amp;").replaceAll("'", "&apos;").replaceAll("^(.+)(<)([^>/a-zA-z_]{1}[^>]*)(>)(.+)$", "$1&lt;$3&gt;$5")
        val reader = XMLInputFactory.newInstance.createXMLStreamReader(new ByteArrayInputStream(document.getBytes()))

        val outValue = new IntWritable(1)
        val venueMap = Utils.getVenueMap()
        var firsttag = true
        var xmlElement = ""
        var venue = ""
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
              } } }
          catch {
            case e: Exception =>
              logger.error("Error parsing XML", e)
          }
        }
        reader.close()
        if (authorCount > 0 && venue != "") {
          for (author_x <- authors) {
            context.write(new Text(author_x+","+venue ), outValue)

          }
        }
      }
      catch {
        case e: Exception =>
          logger.error("Error in parsing XML", e)
          throw new Exception(e)
      }
    }
  }

  class Reduce extends Reducer[Text, IntWritable, Text, Text] {
    var result = new IntWritable
    var top_authors_venue = new mutable.HashMap[String, Integer]()

    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
      var sum = 0
      val scalaValues = values.asScala
      scalaValues.foreach(values => sum += values.get)
      result.set(sum)
      //context.write(key, result)
      top_authors_venue.put(key.toString, result.get())
    }
    override def cleanup(context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
      val grouped = top_authors_venue.groupBy((k) => k._1.toString.split(",")(1))
      grouped.foreach(k => context.write(new Text(k._2.toSeq.sortWith(_._2 > _._2).take(10).toString), new Text(k._1)))

    }


  }


}
