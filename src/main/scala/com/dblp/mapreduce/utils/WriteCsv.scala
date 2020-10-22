package com.dblp.mapreduce.utils
import java.io.{File, PrintWriter}

import scala.io.{BufferedSource, Source}

object WriteCsv {

  def converttocsv_String_String(outputpath : String,jobname:String):Unit={
    val file = Source.fromFile(outputpath+"part-r-00000").getLines
    val csvfile = new PrintWriter(new File(outputpath+jobname+".csv"))
    for (line <- file){
      val lines =line.toString.stripLeading().stripTrailing().split("\t").toSeq
      val venue = lines.last
      val value = lines(0).replace(venue,"").replace(",","")
      csvfile.write(venue+","+value+"\n")
    }
    csvfile.close()
  }
  def converttocsv_String_Int(outputpath : String,jobname:String):Unit={
    val file = Source.fromFile(outputpath+"part-r-00000").getLines
    val csvfile = new PrintWriter(new File(outputpath+jobname+".csv"))
    for (line <- file){
      val lines =line.toString.split("\t").toSeq
      val value = lines.last
      val key = lines(0)
      csvfile.write(key+","+value+"\n")
    }
    csvfile.close()
  }


}
