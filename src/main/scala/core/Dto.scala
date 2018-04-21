package core

case class EventDetails(id: String, name: String, city: String, country: String, payment_required: Int, cat_id: Int, cat_name: String, duration: Long)
 
case class Venue(name: Option[String], address1: Option[String], city: Option[String], state: Option[String], zip: Option[String], country: Option[String], lon: Option[Float], lat: Option[Float])
case class Event(id: String, name: Option[String], eventUrl: Option[String], description: Option[String], duration: Option[Long], rsvpLimit: Option[Int], paymentRequired: Option[Int], status: Option[String])
case class Group(id: Option[String], name: Option[String], city: Option[String], state: Option[String], country: Option[String])
case class Category(name: Option[String], id: Option[Int], shortname: Option[String])

case class Member(memberName: Option[String], memberId: Option[String])
case class MemberEvent(eventId: Option[String], eventName: Option[String], eventUrl: Option[String], time: Option[Long])
