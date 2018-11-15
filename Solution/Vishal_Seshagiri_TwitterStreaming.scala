import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.twitter._
import twitter4j.{FilterQuery, Status}

import scala.util.Random
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.scalatest.BeforeAndAfter
import twitter4j.auth.{Authorization, NullAuthorization}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.ListMap

object TwitterStreaming {
  var window = ListBuffer[Tuple2[Array[String], Int]]()
  var tweetCount = 0

  def processor(rdd_statuses : RDD[Status]): Unit = {
    tweetCount += 1
    var statuses = rdd_statuses.map(_.getText).collect().mkString(" ")
    var hashtags = statuses.split(" ").filter(status => status.startsWith("#")).map(h => h.slice(1, h.size).trim).filter(p => !p.isEmpty)
    if (window.size <= 100) {
      window += ((hashtags, statuses.toString().size))
    } else {
//      if (Random.shuffle(1 to tweetCount).take(100).contains(101)){
      if(Random.nextInt((tweetCount)) < 101) {
        var index_to_replace = Random.shuffle(0 to 99).take(1)(0)
        window(index_to_replace) = ((hashtags, statuses.toString().size))
      }
    }

//    var hash_tag_list = ListBuffer[String]()
    var hash_tag_counter = new mutable.HashMap[String, Integer]().withDefaultValue(0)
    var tweet_length_sum = 0
    for(tweet <- window){
      for(hash_tag <- tweet._1){
//        hash_tag_counter.put(hash_tag, counter+1)
        hash_tag_counter(hash_tag) += 1
      }
      tweet_length_sum += tweet._2
    }
    var avg_tweet_length = tweet_length_sum.toFloat / window.size

    var top5 = ListMap(hash_tag_counter.toSeq.sortWith(_._2 > _._2):_*).take(5)
//    top5.map{case (k, v) => k + ":" + v}.mkString("|")

    println(
      s"""The number of twitter from beginning: ${tweetCount}
       |Top 5 hot hashtags: \n${top5.map{case (k, v) => k + ":" + v}.mkString("\n").trim}
       |The average length of the twitter is: ${avg_tweet_length}
       """.stripMargin)
  }

  def main(args : Array[String]): Unit = {

    val consumerKey = ""
    val consumerSecret = ""
    val accessToken = ""
    val accessTokenSecret = ""

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Authentication of app
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterStreaming")
      .setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val query = new FilterQuery().language("en").track("#")
    val stream = TwitterUtils.createFilteredStream(ssc, None, Some(query))

//    var statuses = stream.map(_.getText)
    stream.foreachRDD(rdd => processor(rdd))

    ssc.start()
    ssc.awaitTermination()
  }
}
