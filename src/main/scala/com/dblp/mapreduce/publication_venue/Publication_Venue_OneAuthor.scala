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

object Publication_Venue_OneAuthor {

  BasicConfigurator.configure()
  val logger: Logger = LoggerFactory.getLogger(Publication_Venue_OneAuthor.getClass)

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
        var publication = ""
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
              }
              if (currentElement eq "title") {
                publication = reader.getElementText
              }
            }
          }
          catch {
            case e: Exception =>
              logger.error("Error parsing XML", e)
          }
        }
        reader.close()
        if (authorCount == 1 && venue != "") {
          val output = new IntWritable(authorCount)
          context.write(new Text(publication + "," + venue), output)
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
      val keys = key.toString.split(",")
      val publication = keys(0)
      val venue = keys(1).toString
      val authorcount = sum
      if (author_count_publication.contains(venue)) {
        mapval = author_count_publication(venue)
        old = mapval.toString.split(",")(0)
        if (old.toInt < authorcount) {
          output = authorcount.toString + "," + publication
          author_count_publication.update(venue, output)
        }
        else if (old.toInt == authorcount) {
          output = mapval.appendedAll("," + publication)
          author_count_publication.update(venue, mapval + "," + publication)
        }
      }
      else {
        author_count_publication.put(venue, authorcount.toString + "," + publication)
      }
    }

    override def cleanup(context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      author_count_publication.foreach(entry => context.write(new Text(entry._1 + "->" + entry._2), new IntWritable(entry._2.split(",")(0).toInt)))

    }


  }

}
