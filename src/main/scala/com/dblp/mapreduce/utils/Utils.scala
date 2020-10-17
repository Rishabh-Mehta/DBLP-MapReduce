package com.dblp.mapreduce.utils

import com.typesafe.config.ConfigFactory

import scala.collection.mutable

object Utils {

  def getVenueMap(): mutable.HashMap[String,String] = {
    val vMap = ConfigFactory.load().getString(ApplicationConstants.VENUE).split(";").toList
    val venueMap = new mutable.HashMap[String,String]()
    for(venue <- vMap){
      val keyval = venue.split(":")
      venueMap.put(keyval(0),keyval(1))
    }
    venueMap
  }

}
