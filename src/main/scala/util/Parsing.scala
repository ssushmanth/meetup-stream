package util

import core._
import org.joda.time.DateTime
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.native.JsonMethods._
import org.joda.time.DateTime
import org.apache.spark.Partitioner
import org.apache.spark.streaming.Seconds
import scala.util.Try

object Parsing {
  
  
  @transient implicit val formats = DefaultFormats
  
  def parseEvent(eventJson: String):Option[EventDetails] ={
    Try({
      val json=parse(eventJson).camelizeKeys
      val event=json.extract[Event]
	  val venue=(json \ "venue").extract[Venue]
      val group=(json \ "group").extract[Group]
	  val category=(json \ "group" \ "category").extract[Category]
	  EventDetails(event.id, event.name.getOrElse(""), venue.city.getOrElse(""), venue.country.getOrElse(""), event.paymentRequired.getOrElse(0), category.id.getOrElse(0), category.shortname.getOrElse(""), event.duration.getOrElse(10800000L))
    }).toOption
  }
  
  def parseHisEvent(eventJson: String):Option[Event] ={
    Try({
      val json=parse(eventJson).camelizeKeys
      val event=json.extract[Event]
	  event
    }).toOption
  }
  
  def parseRsvp(rsvpJson: String)={
    Try({
      val json=parse(rsvpJson).camelizeKeys
      val member=(json \ "member").extract[Member]
      val event=(json \ "event").extract[MemberEvent]
      val response=(json \ "response").extract[String]
      (member, event, response)
    }).toOption
  }
             
}
