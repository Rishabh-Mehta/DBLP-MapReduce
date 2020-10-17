package com.dblp.mapreduce.author_venue

import com.dblp.mapreduce.utils.ApplicationConstants
import com.dblp.mapreduce.utils.ParseUtils
import com.dblp.mapreduce.utils.Utils
import com.typesafe.config.{Config, ConfigFactory}

import org.apache.log4j.BasicConfigurator
import org.slf4j.{Logger, LoggerFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import javax.xml.parsers.SAXParserFactory
import java.io.{ByteArrayInputStream, IOException}
import java.lang

import com.dblp.mapreduce.MapReduce
import javax.xml.stream.XMLInputFactory

import scala.jdk.CollectionConverters.IterableHasAsScala
//import scala.xml.{Elem, XML}

object Author_Venue  {
  BasicConfigurator.configure()
  val logger = LoggerFactory.getLogger(Author_Venue.getClass)

  class Map extends Mapper[LongWritable, Text, Text, IntWritable] {


    override def map(key: LongWritable, value: Text,
                     context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      try {
        val document = ParseUtils.formatXml(value.toString).toString()
        val reader = XMLInputFactory.newInstance.createXMLStreamReader(new ByteArrayInputStream(document.getBytes()))
        var authorCount = 0
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
                  logger.info("Venue " + venue)
                }
                firsttag = false
              }
              val currentElement = reader.getLocalName
              if (currentElement eq "author") {
                authorCount += 1
              }
            }
          }
          catch {
            case e: Exception =>
              logger.error("Error parsing XML", e)
          }
        }
        reader.close()
        if (authorCount > 0 && venue != "") {
          val output = authorCount + "-" + venue
          context.write(new Text(output), outValue)
        }
      }
      catch {
        case e: Exception =>
          logger.error("Error in parsing XML", e)
          throw new Exception(e)
      }
    }
  }
  class Reduce extends Reducer[Text,IntWritable,Text,IntWritable]{
    var result = new IntWritable

    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = 0
      val scalaValues = values.asScala
      scalaValues.foreach(values =>sum += values.get)
      result.set(sum)
      context.write(key,result)
    }


  }


}
