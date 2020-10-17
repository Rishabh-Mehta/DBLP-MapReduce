package com.dblp.mapreduce


import java.io.ByteArrayInputStream
import java.util.concurrent.atomic.AtomicInteger

import com.dblp.mapreduce.author_count.SortbyCoAuthors
import com.dblp.mapreduce.utils.{ParseUtils, Utils}
import com.ctc.wstx.exc.WstxParsingException
import com.dblp.mapreduce.XMLInput.DBLPXmlInputFormat
import com.dblp.mapreduce.author_venue.Author_Venue
import com.dblp.mapreduce.utils.ApplicationConstants
import com.typesafe.config.{Config, ConfigFactory}

import org.slf4j.{Logger, LoggerFactory}
import javax.xml.parsers.SAXParserFactory
import javax.xml.stream.XMLInputFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{FloatWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, KeyValueTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.log4j.BasicConfigurator
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.xml.{Elem, XML}
object MapReduce  {
  BasicConfigurator.configure()
  val logger = LoggerFactory.getLogger(MapReduce.getClass)


  def main(args: Array[String]): Unit = {

    logger.info("Starting Map Reduce Jobs")
    val configObj = ConfigFactory.load().getConfig(ApplicationConstants.MAP_REDUCE_CONFIG)
    val inputpath = ConfigFactory.load().getString(ApplicationConstants.INPUT_PATH)
    val jobs = ConfigFactory.load().getString(ApplicationConstants.JOBS).split(",").toList

    jobs.foreach(job => {
      if (job.trim.equals("AuthorCount")) {
        logger.info("Starting Co-Author Count Job")
        //AuthorCount.runJob(configObj.inputFile, configObj.outputFile)
        val configuration = new Configuration
        val job1 = Job.getInstance(configuration,job)
        val inputPath = new Path("~/Downloads/input")
        val outputPathFirstJob = new Path("~/Downloads/output")
        outputPathFirstJob.getFileSystem(configuration).delete(outputPathFirstJob, true)
        logger.info("Here")
        job1.setJarByClass(this.getClass)
        job1.setMapperClass(classOf[com.dblp.mapreduce.author_venue.Author_Venue.Map])
        job1.setReducerClass(classOf[com.dblp.mapreduce.author_venue.Author_Venue.Reduce])
        job1.setInputFormatClass(classOf[DBLPXmlInputFormat])
        job1.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
        job1.setMapOutputKeyClass(classOf[Text])
        job1.setMapOutputValueClass(classOf[IntWritable])
        job1.setOutputKeyClass(classOf[Text])
        job1.setOutputValueClass(classOf[IntWritable])
        FileInputFormat.addInputPath(job1,inputPath)
        FileOutputFormat.setOutputPath(job1, outputPathFirstJob)
        System.exit(if (job1.waitForCompletion(true)) 0
        else 1)



      } else if (job.trim.equals("PublicationYearStratification")) {
        logger.info("Starting Co-Author count, Stratification by Publication Year job")
        //PublicationYearStratification.runJob(configObj.inputFile, configObj.outputFile)
      } else if (job.trim.equals("PublicationVenueStratification")) {
        logger.info("Starting Co-Author count, Stratification by Publication Venue job")
        //PublicationVenueStratification.runJob(configObj.inputFile, configObj.outputFile)
      } else if (job.trim.equals("Authorship")) {
        logger.info("Starting Authorship and Top 100 and least 100 collaborating authors job")
        //Authorship.runJob(configObj.inputFile, configObj.outputFile)
      }
      else if (job.trim.equals("MeanMedianStatistics")) {
        logger.info("Starting MeanMedianStatistics calculation job")
        //MeanMedianStatistics.runJob(configObj.inputFile, configObj.outputFile)
      }
      else if (job.trim.equals("MeanMedianStatisticsWithStratification")) {
        logger.info("Starting MeanMedianStatisticsWithStratification calculation job")
        //MeanMedianStatisticsWithStratification.runJob(configObj.inputFile, configObj.outputFile)
      }
    })
    logger.info("Completed All Jobs Successfully")
  }
}




