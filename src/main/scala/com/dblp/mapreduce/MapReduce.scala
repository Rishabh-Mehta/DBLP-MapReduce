package com.dblp.mapreduce

import com.dblp.mapreduce.XMLInput.XmlInputFormat
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
import org.slf4j.{Logger, LoggerFactory}



object MapReduce {
  BasicConfigurator.configure()
  val logger: Logger = LoggerFactory.getLogger(MapReduce.getClass)


  def main(args: Array[String]): Unit = {

    logger.info("Starting Map Reduce Jobs")
    //val inputPath = new Path(ConfigFactory.load().getString(ApplicationConstants.INPUT_PATH))
    val inputPath = new Path(args(0))
    val outputPath = args(1)
    val jobs = ConfigFactory.load().getString(ApplicationConstants.JOBS).split(",").toList

    jobs.foreach(jobname => {

      val configuration = new Configuration
      val job = Job.getInstance(configuration, jobname)
      job.setNumReduceTasks(1)
      configuration.set("mapred.output.textoutputformat.separator", ",")
      val outputPathFirstJob = new Path(args(1).toString+jobname)
      //val outputPathFirstJob = new Path("s3://com.dblp.mapreduce.rmehta35/output/"+ jobname)
      //val outputPathFirstJob = new Path("/home/hadoop/output/"+ jobname)
      outputPathFirstJob.getFileSystem(configuration).delete(outputPathFirstJob, true)
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[IntWritable])
      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[IntWritable])
      job.setInputFormatClass(classOf[XmlInputFormat])

      FileInputFormat.addInputPath(job, inputPath)
      FileOutputFormat.setOutputPath(job, outputPathFirstJob)

      if (jobname.trim.equals("Top_Authors_Published_Venue")) {
        logger.info("Starting Author_Publications Top 10 Authors with highest publications at the venue")
        job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
        job.setJarByClass(author_publication.Top_Authors_Published_Venue.getClass)
        job.setMapperClass(classOf[com.dblp.mapreduce.author_publication.Top_Authors_Published_Venue.Map])
        job.setReducerClass(classOf[com.dblp.mapreduce.author_publication.Top_Authors_Published_Venue.Reduce])
        job.waitForCompletion(true)



      } else if (jobname.trim.equals("PublicationVenueOneAuthor")) {
        logger.info("Starting publication_venue , Publications at each Venue with one Author")
        job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
        job.setJarByClass(com.dblp.mapreduce.publication_venue.Publication_Venue_OneAuthor.getClass)
        job.setMapperClass(classOf[com.dblp.mapreduce.publication_venue.Publication_Venue_OneAuthor.Map])
        job.setReducerClass(classOf[com.dblp.mapreduce.publication_venue.Publication_Venue_OneAuthor.Reduce])
        job.waitForCompletion(true)


      } else if (jobname.trim.equals("Publication_Highest_Authors_Venues")) {
        logger.info("Starting publication_venue , Publication with Highest Authors at Venues")
        job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
        job.setJarByClass(Publication_Venue_HighestAuthor.getClass)
        job.setMapperClass(classOf[com.dblp.mapreduce.publication_venue.Publication_Venue_HighestAuthor.Map])
        job.setReducerClass(classOf[com.dblp.mapreduce.publication_venue.Publication_Venue_HighestAuthor.Reduce])
        job.waitForCompletion(true)

      } else if (jobname.trim.equals("Author_Most_Coauthor")) {
        logger.info("Starting AuthorStats  and Top 100 collaborating authors job")
        job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
        job.setJarByClass(com.dblp.mapreduce.author_stats.Author_Most_Coauthor.getClass)
        job.setMapperClass(classOf[com.dblp.mapreduce.author_stats.Author_Most_Coauthor.Map])
        job.setReducerClass(classOf[com.dblp.mapreduce.author_stats.Author_Most_Coauthor.Reduce])
        job.waitForCompletion(true)
      }
      else if (jobname.trim.equals("Author_No_Coauthor")) {
        logger.info("Starting AuthorStats  Least 100 collaborating authors job")
        job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
        job.setJarByClass(com.dblp.mapreduce.author_stats.Author_No_Coauthor.getClass)
        job.setMapperClass(classOf[com.dblp.mapreduce.author_stats.Author_No_Coauthor.Map])
        job.setReducerClass(classOf[com.dblp.mapreduce.author_stats.Author_No_Coauthor.Reduce])
        job.waitForCompletion(true)
      }

      else if (jobname.trim.equals("Author_Published_Consecutively")) {
        logger.info("Starting Author_Years  Authors publishing without interruption for N >=10 years")
        job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
        job.setJarByClass(com.dblp.mapreduce.author_years.Author_Published_Consecutively.getClass)
        job.setMapperClass(classOf[com.dblp.mapreduce.author_years.Author_Published_Consecutively.Map])
        job.setReducerClass(classOf[com.dblp.mapreduce.author_years.Author_Published_Consecutively.Reduce])
        job.waitForCompletion(true)

      }

    })
    logger.info("Completed All Jobs Successfully")
    System.exit(1)
  }
}




