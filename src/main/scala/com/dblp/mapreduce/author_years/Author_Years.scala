package com.dblp.mapreduce.author_years

import java.io.ByteArrayInputStream
import java.{lang, util}

import com.dblp.mapreduce.author_stats.Author_Top
import javax.xml.stream.XMLInputFactory
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.apache.log4j.BasicConfigurator
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.jdk.CollectionConverters.IterableHasAsScala




object Author_Years {

  BasicConfigurator.configure()
  val logger: Logger = LoggerFactory.getLogger(Author_Top.getClass)
  class Map extends Mapper[LongWritable, Text, Text, IntWritable] {
    override def map(key: LongWritable, value: Text,context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      try {
        var document = value.toString
        document = document.replaceAll("\n", "").replaceAll("&", "&amp;").replaceAll("'", "&apos;").replaceAll("^(.+)(<)([^>/a-zA-z_]{1}[^>]*)(>)(.+)$", "$1&lt;$3&gt;$5")
        val reader = XMLInputFactory.newInstance.createXMLStreamReader(new ByteArrayInputStream(document.getBytes()))
        var pub_year = ""
        var authors = new util.ArrayList[String]()
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
                  author = reader.getElementText
                  if (author.toString != "") {
                    authorCount += 1
                    authors.add(author)
                  }
              }
              else if (currentElement eq "year") {
                pub_year = reader.getElementText
              //  logger.info("Publication Read "+pub_year+" "+authors)

              }
            }
          }
          catch {
            case e: Exception =>
              logger.error("Error parsing XML", e)
          }
        }
        reader.close()
        if (authorCount > 0 && pub_year != "") {
          val output = new IntWritable(pub_year.toInt)
          for (author_x <- authors.asScala) {
            context.write(new Text(author_x.toString ), output)
            //logger.info("Mapper Output "+author_x.toString+" "+output)
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
      scalaValues.toSeq.sliding(2).foreach{
        case y1 :: tail => if(tail.nonEmpty){
          val y2 = tail.head
          if(y2 -y1 == 1)
            ranges(ranges.size -1) +=1
          else
            ranges +=1
        }
      }
      //logger.info("Reducer Input ,"+key.toString+" , "+scalaValues+" "+ranges.max)
      if(ranges.max >= 10){
        context.write(key,new IntWritable(ranges.max))
      }

      //author_published_years.put(key.toString,scalaValues)
    }
  }

}
