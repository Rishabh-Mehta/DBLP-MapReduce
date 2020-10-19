package com.dblp.mapreduce

import com.dblp.mapreduce.XMLInput.XmlInputFormat
import com.dblp.mapreduce.author_venue.Author_Venue
import com.dblp.mapreduce.publication_venue.Publication_Venue_HighestAuthor
import com.dblp.mapreduce.utils.ApplicationConstants
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.log4j.BasicConfigurator
import org.slf4j.LoggerFactory

object MapReduce {
  BasicConfigurator.configure()
  val logger = LoggerFactory.getLogger(MapReduce.getClass)


  def main(args: Array[String]): Unit = {

    logger.info("Starting Map Reduce Jobs")
    val configObj = ConfigFactory.load().getConfig(ApplicationConstants.MAP_REDUCE_CONFIG)
    val inputpath = ConfigFactory.load().getString(ApplicationConstants.INPUT_PATH)
    val jobs = ConfigFactory.load().getString(ApplicationConstants.JOBS).split(",").toList

    jobs.foreach(jobname => {
      if (jobname.trim.equals("AuthorCount")) {
        //logger.info("Starting Co-Author Count Job")
        //AuthorCount.runJob(configObj.inputFile, configObj.outputFile)

//        val configuration = new Configuration
//        val job = Job.getInstance(configuration, jobname)
//        configuration.set("mapreduce.output.textoutputformat.separator", ",")
//        val inputPath = new Path("home/hadoop/input")
//        val outputPathFirstJob = new Path("home/hadoop/output/"+jobname)
//        outputPathFirstJob.getFileSystem(configuration).delete(outputPathFirstJob, true)
//        job.setJarByClass(Author_Venue.getClass)
//        job.setMapperClass(classOf[com.dblp.mapreduce.author_venue.Author_Venue.Map])
//        job.setReducerClass(classOf[com.dblp.mapreduce.author_venue.Author_Venue.Reduce])
//        job.setOutputKeyClass(classOf[Text])
//        job.setOutputValueClass(classOf[IntWritable])
//        job.setMapOutputKeyClass(classOf[Text])
//        job.setMapOutputValueClass(classOf[IntWritable])
//        job.setInputFormatClass(classOf[XmlInputFormat])
//        job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
//        FileInputFormat.addInputPath(job, inputPath)
//        FileOutputFormat.setOutputPath(job, outputPathFirstJob)
//        System.exit(if (job.waitForCompletion(true)) 0
//        else 1)


//      } else if (jobname.trim.equals("PublicationVenueOneAuthor")) {
//        logger.info("Starting publication venue , Publications at each Venue with one Author")
//        val configuration = new Configuration
//        val job = Job.getInstance(configuration, jobname)
//        configuration.set("mapreduce.output.textoutputformat.separator", ",")
//        val inputPath = new Path("home/hadoop/input")
//        val outputPathFirstJob = new Path("home/hadoop/output/"+jobname)
//        outputPathFirstJob.getFileSystem(configuration).delete(outputPathFirstJob, true)
//        job.setJarByClass(Author_Venue.getClass)
//        job.setMapperClass(classOf[com.dblp.mapreduce.publication_venue.Publication_Venue_OneAuthor.Map])
//        job.setReducerClass(classOf[com.dblp.mapreduce.publication_venue.Publication_Venue_OneAuthor.Reduce])
//        job.setOutputKeyClass(classOf[Text])
//        job.setOutputValueClass(classOf[IntWritable])
//        job.setMapOutputKeyClass(classOf[Text])
//        job.setMapOutputValueClass(classOf[IntWritable])
//        job.setInputFormatClass(classOf[XmlInputFormat])
//        job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
//        FileInputFormat.addInputPath(job, inputPath)
//        FileOutputFormat.setOutputPath(job, outputPathFirstJob)
//        System.exit(if (job.waitForCompletion(true)) 0
//        else 1)


      } else if (jobname.trim.equals("PublicationwithHighestAuthorsatVenues")) {
//        logger.info("Starting publication_venue , Publication with Highest Authors at Venues")
//        val configuration = new Configuration
//        val job = Job.getInstance(configuration, jobname)
//        configuration.set("mapreduce.output.textoutputformat.separator", ",")
//        val inputPath = new Path("home/hadoop/input")
//        val outputPathFirstJob = new Path("home/hadoop/output/"+jobname)
//        outputPathFirstJob.getFileSystem(configuration).delete(outputPathFirstJob, true)
//        job.setJarByClass(Publication_Venue_HighestAuthor.getClass)
//        job.setMapperClass(classOf[com.dblp.mapreduce.publication_venue.Publication_Venue_HighestAuthor.Map])
//        job.setReducerClass(classOf[com.dblp.mapreduce.publication_venue.Publication_Venue_HighestAuthor.Reduce])
//        job.setOutputKeyClass(classOf[Text])
//        job.setOutputValueClass(classOf[IntWritable])
//        job.setMapOutputKeyClass(classOf[Text])
//        job.setMapOutputValueClass(classOf[IntWritable])
//        job.setInputFormatClass(classOf[XmlInputFormat])
//        job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
//        FileInputFormat.addInputPath(job, inputPath)
//        FileOutputFormat.setOutputPath(job, outputPathFirstJob)
//        System.exit(if (job.waitForCompletion(true)) 0
//        else 1)

      } else if (jobname.trim.equals("AuthorStats_TOP")) {
//        logger.info("Starting AuthorStats  and Top 100 collaborating authors job")
//        val configuration = new Configuration
//        val job = Job.getInstance(configuration, jobname)
//        configuration.set("mapreduce.output.textoutputformat.separator", ",")
//        val inputPath = new Path("home/hadoop/input")
//        val outputPathFirstJob = new Path("home/hadoop/output/"+jobname)
//        outputPathFirstJob.getFileSystem(configuration).delete(outputPathFirstJob, true)
//        job.setJarByClass(Author_Venue.getClass)
//        job.setMapperClass(classOf[com.dblp.mapreduce.author_stats.Author_Top.Map])
//        job.setReducerClass(classOf[com.dblp.mapreduce.author_stats.Author_Top.Reduce])
//        job.setOutputKeyClass(classOf[Text])
//        job.setOutputValueClass(classOf[IntWritable])
//        job.setMapOutputKeyClass(classOf[Text])
//        job.setMapOutputValueClass(classOf[IntWritable])
//        job.setInputFormatClass(classOf[XmlInputFormat])
//        job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
//        FileInputFormat.addInputPath(job, inputPath)
//        FileOutputFormat.setOutputPath(job, outputPathFirstJob)
//        System.exit(if (job.waitForCompletion(true)) 0
//        else 1)
      }
      else if (jobname.trim.equals("AuthorStats_LOW")) {
        logger.info("Starting AuthorStats  Least 100 collaborating authors job")
        val configuration = new Configuration
        val job = Job.getInstance(configuration, jobname)
        configuration.set("mapreduce.output.textoutputformat.separator", ",")
        val inputPath = new Path("home/hadoop/input")
        val outputPathFirstJob = new Path("home/hadoop/output/"+jobname)
        outputPathFirstJob.getFileSystem(configuration).delete(outputPathFirstJob, true)
        job.setJarByClass(Author_Venue.getClass)
        job.setMapperClass(classOf[com.dblp.mapreduce.author_stats.Author_Low.Map])
        job.setReducerClass(classOf[com.dblp.mapreduce.author_stats.Author_Low.Reduce])
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[IntWritable])
        job.setMapOutputKeyClass(classOf[Text])
        job.setMapOutputValueClass(classOf[IntWritable])
        job.setInputFormatClass(classOf[XmlInputFormat])
        job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
        FileInputFormat.addInputPath(job, inputPath)
        FileOutputFormat.setOutputPath(job, outputPathFirstJob)
        System.exit(if (job.waitForCompletion(true)) 0
        else 1)
      }

      else if (jobname.trim.equals("MeanMedianStatistics")) {
        logger.info("Starting MeanMedianStatistics calculation job")
        //MeanMedianStatistics.runJob(configObj.inputFile, configObj.outputFile)
      }
      else if (jobname.trim.equals("MeanMedianStatisticsWithStratification")) {
        logger.info("Starting MeanMedianStatisticsWithStratification calculation job")
        //MeanMedianStatisticsWithStratification.runJob(configObj.inputFile, configObj.outputFile)
      }
    })
    logger.info("Completed All Jobs Successfully")
  }
}




