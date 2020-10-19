package com.dblp.mapreduce.author_publication

import java.io.ByteArrayInputStream
import java.lang

import com.dblp.mapreduce.utils.{ParseUtils, Utils}
import javax.xml.stream.XMLInputFactory
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.apache.log4j.BasicConfigurator
import org.slf4j.LoggerFactory

import scala.collection.View.Empty.take
import scala.collection.mutable
import scala.jdk.CollectionConverters.IterableHasAsScala

object Top_Author_Venue {
  BasicConfigurator.configure()
  val logger = LoggerFactory.getLogger(Top_Author_Venue.getClass)
  class Map extends Mapper[LongWritable, Text, Text, IntWritable] {
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      try {
        var document = value.toString
        document = document.replaceAll("\n", "").replaceAll("&", "&amp;").replaceAll("'", "&apos;").replaceAll("^(.+)(<)([^>/a-zA-z_]{1}[^>]*)(>)(.+)$", "$1&lt;$3&gt;$5")
        val reader = XMLInputFactory.newInstance.createXMLStreamReader(new ByteArrayInputStream(document.getBytes()))
        var authorCount = 0
        val outValue = new IntWritable(1)
        val venueMap = Utils.getVenueMap()
        var firsttag = true
        var xmlElement = ""
        var venue = ""
        var author =""
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
              val currentElement = reader.getLocalName
              if (currentElement eq "author") {
                authorCount += 1
                author = reader.getElementText
                context.write(new Text(author+","+venue),outValue)
                logger.debug("Mapper Output "+author+" , "+venue+" :",outValue)
              }
            }
          }
          catch {
            case e: Exception =>
              logger.error("Error parsing XML", e)
          }
        }
        reader.close()
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
      logger.info(key.toString)
      logger.debug("\n" + "+Reducer Output " + key + ":" + result + "\n")
      //context.write(key, result)
      top_authors_venue.put(key.toString, result.get())
    }

    override def cleanup(context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
      val grouped = top_authors_venue.groupBy((k) => k._1.toString.split(",")(1))
      grouped.foreach(k => context.write(new Text(k._2.toSeq.sortWith(_._2 > _._2).take(10).toString), new Text(k._1)))

    }


  }



}
