import com.dblp.mapreduce.utils.Utils
import org.scalatest.FunSuite
import org.scalatest._
import com.typesafe.config
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import javax.xml.parsers.SAXParserFactory

import scala.xml.XML

class UnitTest extends FunSuite {

  test("Reading config values from config file") {
    val actual = ConfigFactory.load().getString("JobsToRunTest")
    val expected = "TestValue"
    assert(actual == expected)
  }
  test("Invalid configName") {

    assertThrows[ConfigException] {
      ConfigFactory.load().getString("JobsToRunTest1")
    }
  }
  test("Check Venue Map") {
    val venueMap = Utils.getVenueMap()
    val values = Array("journal", "book", "school", "webpage")
    assert(venueMap.size == 8)
    venueMap.values.foreach(value => {
      assert(values.contains(value))
    })
  }
  test("XML Parsing"){
     val xmlParser = SAXParserFactory.newInstance().newSAXParser()
     val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI

    val text = "<phdthesis mdate=\"2019-07-25\" key=\"phd/basesearch/Shih17\">\n     <author>Mimosa Networks</author>\n   <author>Chin Shu</author>\n <author>Xu Shin</author>\n <author>Min Wang</author>\n   <title>Algorithms and protocols for next generation WiFi networks.</title>\n        <year>2017</year>\n        <school>Georgia Institute of Technology, Atlanta, GA, USA</school>\n        <ee>http://hdl.handle.net/1853/58204</ee>\n        <ee>https://www.base-search.net/Record/9d30ea9fbee665fc1765cecd4c3d85af7b4282bc5b07a0e6f46ac261e00fb871</ee>\n        <note type=\"source\">base-search.net (ftgeorgiatech:oai:smartech.gatech.edu:1853/58204)</note>\n    </phdthesis>"
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dtdFilePath"><dblp>$text</dblp>"""
    val element = XML.withSAXParser(xmlParser).loadString(xmlString)

      val publication = (element \\ "title").text.toString
      assert(publication =="Algorithms and protocols for next generation WiFi networks." )
    }
  test("Extract Number of CoAuthors"){
    val xmlParser = SAXParserFactory.newInstance().newSAXParser()
    val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI

    val text = "<phdthesis mdate=\"2019-07-25\" key=\"phd/basesearch/Shih17\">\n     <author>Mimosa Networks</author>\n   <author>Chin Shu</author>\n <author>Xu Shin</author>\n <author>Min Wang</author>\n   <title>Algorithms and protocols for next generation WiFi networks.</title>\n        <year>2017</year>\n        <school>Georgia Institute of Technology, Atlanta, GA, USA</school>\n        <ee>http://hdl.handle.net/1853/58204</ee>\n        <ee>https://www.base-search.net/Record/9d30ea9fbee665fc1765cecd4c3d85af7b4282bc5b07a0e6f46ac261e00fb871</ee>\n        <note type=\"source\">base-search.net (ftgeorgiatech:oai:smartech.gatech.edu:1853/58204)</note>\n    </phdthesis>"
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dtdFilePath"><dblp>$text</dblp>"""
    val element = XML.withSAXParser(xmlParser).loadString(xmlString)

    val authors = (element \\ "author").map(author => author.text.toLowerCase.trim).toList.sorted
    assert(authors.size ==4 )

  }
  }




