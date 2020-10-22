package com.dblp.mapreduce.utils
import scala.io.{BufferedSource, Source}

object WriteCsv {

  def converttocsv(outputpath : String):Unit={
    val file = Source.fromFile(outputpath+"/part-r-00000").getLines()
    print(file)
  }

}
