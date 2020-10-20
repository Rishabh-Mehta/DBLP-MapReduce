package com.dblp.mapreduce.author_stats

import java.lang
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.apache.log4j.BasicConfigurator
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable
import scala.jdk.CollectionConverters.IterableHasAsScala

/**
 * Task 5a   List 0f 100 Authors who publish with most co-authors in descending order.
 * Mapper  : Maps ((Author,Publication),Author_Count) for each publication
 * Reducer : Receives the mapper input from multiple publications and combines multiple inputs with same key and puts
 *           them in a HashMap[String, String] with key Author and value of Author_Count,publication
 * Cleanup : This receives the HashMap and we reformat it to make Author_Count as key and we sort with key in descending
 *           order and select top 100 from the resulting list.
 */

object Author_Top {

  BasicConfigurator.configure()
  val logger: Logger = LoggerFactory.getLogger(Author_Top.getClass)
  class Map extends Mapper[LongWritable, Text, Text, IntWritable] {
    override def map(key: LongWritable, value: Text,context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      val fileDtd = getClass.getClassLoader.getResource("dblp.dtd").toURI
      val inputXml =
        s"""<?xml version="1.0" encoding="ISO-8859-1"?>
      <!DOCTYPE dblp SYSTEM "$fileDtd">
      <dblp>""" + value.toString + "</dblp>"

      val preprocessedXML = xml.XML.loadString(inputXml)
      val authors = (preprocessedXML \\ "author").map(author => author.text.toLowerCase.trim).toList.sorted
      val publication = (preprocessedXML \\ "title")
      val authorCount = authors.size

      if (authorCount > 0 && publication != "") {
          val output = new IntWritable(authorCount)
          for (author_x <- authors) {
            context.write(new Text(author_x.toString + "," + publication), output)
          }
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
      val top100 = coauthor_sort.toSeq.sortWith(_._2 > _._2).take(100)
      top100.foreach(ent => context.write(new Text(ent._1.toString), new IntWritable(ent._2)))

    }
  }


}
