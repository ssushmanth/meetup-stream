package jobs

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import receiver.MeetupReceiver
import core.Loggable
import scala.io.Source
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import core._


object MeetupJob extends App{
  
  import util.Parsing._
  import org.apache.spark.streaming.StreamingContext._
  import core.Loggable._
  import transformations.FeatureExtraction._

  val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("MeetupExperiments")
      .set("spark.executor.memory", "1g")
      .set("spark.driver.memory", "1g")
      
  Loggable.setStreamingLogLevels()
  
  
  val ssc=new StreamingContext(conf, Seconds(2))
  val eventStream = ssc.receiverStream(new MeetupReceiver("http://stream.meetup.com/2/open_events")).flatMap(parseEvent)    
  //static
  //eventStream.print()
  
  val windowEventStream = eventStream.window(Seconds(10),Seconds(10))
  ssc.checkpoint("checkpoints")
  windowEventStream.cache()
  
  def updateSumFunc(values: Seq[Int], state: Option[Int]): Option[Int] = {
    val currentCount = values.sum
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)
  }
  
  def updateSumFunc2f(values: Seq[Double], state: Option[Double]): Option[Double] = {
    val currentCount = values.sum
    val previousCount = state.getOrElse(0.0)
    Some(currentCount + previousCount)
  }
  
  val cityEventsStream = windowEventStream.filter{event => event.city == "New York"}.map{event => (event.city,1)}.reduceByKey(_+_).updateStateByKey(updateSumFunc _)
  cityEventsStream.foreachRDD(rdd => {rdd.foreach{case (city, count) => println("No. of Events happening in %s city::%s".format(city, count))}})
  
  val freeEventsStream = windowEventStream.filter{event => event.payment_required == 0}.map{event => ("Free",1)}.reduceByKey(_+_).updateStateByKey(updateSumFunc _)
  freeEventsStream.foreachRDD(rdd => {rdd.foreach{case (free, count) => println("No. of Free Events happening::%s".format(count))}})

  val techEventsStream = windowEventStream.filter{event => event.cat_name == "tech"}
  var techCount = 0
  val countTexhEventsStream = techEventsStream.filter{event => event.cat_name == "tech"}.map{event => (event.cat_name,1)}.reduceByKey(_+_).updateStateByKey(updateSumFunc _)
  countTexhEventsStream.foreachRDD(rdd => {rdd.foreach{case (cat_name, count) => techCount = count; println("No. of %s Events happening::%s".format(cat_name,count))}})
  
  val bigDataUSEventsStream = windowEventStream.filter{event => event.country == "us" && event.name.toLowerCase.indexOf("big data") >= 0}.map{event => ("Big Data",1)}.reduceByKey(_+_).updateStateByKey(updateSumFunc _)
  bigDataUSEventsStream.foreachRDD(rdd => {rdd.foreach{case (name, count) => println("No. of %s Events happening in US::%s".format(name,count))}})
  
  val sumDurTechEventsStream = techEventsStream.map{event => (event.cat_name + " Events", event.duration.toDouble / 60000.0)}.reduceByKey(_+_).updateStateByKey(updateSumFunc2f _)
  sumDurTechEventsStream.foreachRDD(rdd => {
	rdd.map{case(x:String, y:Double) => (x, y / techCount.toDouble)}.foreach{case (cat_name:String, avg:Double) => {
			val hrs = (avg / 60.0).toInt
			val min = (avg % 60).toInt
			println("Avg duration of %s happening::%d hours %d minutes".format(cat_name,hrs,min))
		}
	}
  })

  val eventsHistory = ssc.sparkContext.textFile("data/events/events.json", 1).flatMap(parseHisEvent)
  val rsvpHistory = ssc.sparkContext.textFile("data/rsvps/rsvps.json", 1).flatMap(parseRsvp)
//eventsHistory.take(20).foreach(println)
//rsvpHistory.take(20).foreach(println) 
  val localDictionary=Source
    .fromURL(getClass.getResource("/wordsEn.txt"))
    .getLines
    .zipWithIndex
    .toMap
  
  val dictionary= ssc.sparkContext.broadcast(localDictionary)
        
  val eventVectors=eventsHistory.flatMap{
      event=>eventToVector(dictionary.value,event.description.getOrElse(""))
  }
  
  eventVectors.cache()

  val eventCount=eventVectors.count()
  
  println(s"Training on ${eventCount} events")
   
  val eventClusters = KMeans.train(eventVectors, 10, 2)
  //end static
  
  println(s"Event Training Complete")
      
  val eventHistoryById=eventsHistory
    .map{event=>(event.id, event.description.getOrElse(""))}
    .reduceByKey{(first: String, second: String)=>first}
  
  println(s"Joining with ${eventHistoryById.count}")
  
  eventHistoryById.cache()  
 
  val membersByEventId=rsvpHistory
     .flatMap{
       case(member, memberEvent, response) => 
         memberEvent.eventId.map{id=>(id,(member, response))}
     }
  
  val rsvpEventInfo=membersByEventId.join(eventHistoryById)

  val memberEventInfo = rsvpEventInfo.flatMap{
    case(eventId, ((member, response), description)) => {
      eventToVector(dictionary.value,description).map{ eventVector=> 
        val eventCluster=eventClusters.predict(eventVector)
        (eventCluster,(member, response))
      }
    }
  }
  
//memberEventInfo.take(20).foreach(println)  
  val memberGroups = memberEventInfo.filter{case(cluster, (member, memberResponse)) => memberResponse == "yes"}.map{case(cluster, (member, memberResponse)) => (cluster,member)}.groupByKey().map{case(cluster,memberItr) => (cluster,memberItr.toSet)}
  
//memberGroups.take(20).foreach(println)
  
  val recomendations=memberEventInfo
    .join(memberGroups)
    .map{case(cluster, ((member, memberResponse), members)) => (member.memberName, members-member)}
  
  recomendations.take(10).foreach(println) 
  
  ssc.start
  ssc.awaitTermination()
  
}
