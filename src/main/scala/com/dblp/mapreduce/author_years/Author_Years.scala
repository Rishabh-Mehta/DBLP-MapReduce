package com.dblp.mapreduce.author_years

import java.lang

import com.dblp.mapreduce.author_stats.Author_Top
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.apache.log4j.BasicConfigurator
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * Task 5b   List of Authors who published without interruption for N >= 10 Years for each venue
 * Mapper  : Maps (Author,Publication_Year) for each publication
 * Reducer : Receives the mapper input from multiple publications and combines multiple inputs with same key.Here we check
 *           if a Author has consecutively published for more than 10 years and emit (Author , No_of_years_published)
 */


object Author_Years {

  BasicConfigurator.configure()
  val logger: Logger = LoggerFactory.getLogger(Author_Years.getClass)

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
        val pub_year = (preprocessedXML \\ "@year").toString()
        val authorCount = authors.size

        if (authorCount > 0 && pub_year != "") {
          val output = new IntWritable(pub_year.toInt)

          for (author_x <- authors) {
            context.write(new Text(author_x.toString), output)


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
    //var author_published_years = new mutable.HashMap[String,mutable.TreeSet[Int]]()
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val scalaValues = new mutable.TreeSet[Int]()
      values.forEach(value => scalaValues.add(value.toString.toInt))
      var ranges = mutable.ArrayBuffer[Int](1)
      scalaValues.toSeq.sliding(2).foreach {
        case y1 :: tail => if (tail.nonEmpty) {
          val y2 = tail.head
          if (y2 - y1 == 1)
            ranges(ranges.size - 1) += 1
          else
            ranges += 1
        }
      }
      //logger.info("Reducer Input ,"+key.toString+" , "+scalaValues+" "+ranges.max)
      if (ranges.max >= 10) {
        context.write(key, new IntWritable(ranges.max))
      }

      //author_published_years.put(key.toString,scalaValues)
    }
  }

}
