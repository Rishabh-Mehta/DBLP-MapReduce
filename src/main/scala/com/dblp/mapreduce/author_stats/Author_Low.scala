package com.dblp.mapreduce.author_stats

import java.io.ByteArrayInputStream
import java.lang

import javax.xml.stream.XMLInputFactory
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.apache.log4j.BasicConfigurator
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.jdk.CollectionConverters.IterableHasAsScala

object Author_Low {

  BasicConfigurator.configure()
  val logger: Logger = LoggerFactory.getLogger(Author_Low.getClass)

  class Map extends Mapper[LongWritable, Text, Text, IntWritable] {

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      try {
        var document = value.toString
        document = document.replaceAll("\n", "").replaceAll("&", "&amp;").replaceAll("'", "&apos;").replaceAll("^(.+)(<)([^>/a-zA-z_]{1}[^>]*)(>)(.+)$", "$1&lt;$3&gt;$5")
        val reader = XMLInputFactory.newInstance.createXMLStreamReader(new ByteArrayInputStream(document.getBytes()))
        var firstauth = true
        var publication = ""
        var authors = ""
        var authorCount = 0
        var firstTag = true
        while (reader.hasNext) {
          var author = ""
          try {
            reader.next
            if (reader.isStartElement) {
              if (firstTag) {
                firstTag = false
              }
              val currentElement = reader.getLocalName
              if (currentElement eq "author") {
                if (firstauth) {
                  author = reader.getElementText
                  if (author.toString != "") {
                    authors = author
                  }
                  authorCount += 1
                  firstauth = false
                }
                else {
                  authorCount += 1
                  author = reader.getElementText
                  if (author.toString != "") {
                    authors = authors + "," + author
                  }
                }
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
        if (authorCount > 0 && publication != "") {
          val output = new IntWritable(authorCount)
          for (author_x <- authors.split(",")) {
            context.write(new Text(author_x.toString + "," + publication), output)
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

  class Reduce extends Reducer[Text, IntWritable, Text, IntWritable] {
    var coauthor_count_publication = new mutable.TreeMap[String, String]()

    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = 0
      val scalaValues = values.asScala
      scalaValues.foreach(values => sum += values.get)
      val authorcount = sum
      val keys = key.toString.split(",")
      val author = keys(0)
      val publication = keys(1)
      var mapval = ""
      var old = ""
      var output = ""
      if (coauthor_count_publication.contains(author)) {
        mapval = coauthor_count_publication(author)
        old = mapval.split(",")(0)
        if (old.toInt < authorcount) {
          output = authorcount.toString + "," + publication
          coauthor_count_publication.update(author, output)
        }
        else if (old.toInt == sum) {
          output = mapval.toString + "," + publication
          coauthor_count_publication.update(author, output)
        }
      }
      else {
        output = authorcount.toString + "," + publication
        coauthor_count_publication.put(author, output)
      }
    }

    override def cleanup(context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val coauthor_sort = new mutable.HashMap[String, Integer]()
      coauthor_count_publication.foreach(entry => coauthor_sort.put(entry._1 + "," + entry._2.split(",")(1), entry._2.split(",")(0).toInt))
      val top100 = coauthor_sort.toSeq.sortWith(_._2 < _._2).take(100)
      top100.foreach(ent => context.write(new Text(ent._1.toString), new IntWritable(ent._2)))

    }
  }


}
