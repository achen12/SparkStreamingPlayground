/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package streamingtests.twitter

import scala.collection.mutable.SynchronizedQueue
import org.apache.spark.streaming.{Minutes,Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging
import twitter4j.Status
import org.apache.spark.rdd.RDD


/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 * Run this on your local machine as
 *
 */

object TwitterReplay {
  def main(args: Array[String]) {

    val consumerKey = TwitterCred.consumerKey
    val consumerSecret = TwitterCred.consumerSecret
    val accessToken = TwitterCred.accessToken
    val accessTokenSecret = TwitterCred.accessTokenSecret

    StreamingExamples.setStreamingLogLevels()

    val filters = Array("#KCA")

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf()
        .setAppName("TwitterReplay-Spark")
        .setMaster("local[2]")
    
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val sc = ssc.sparkContext
    val stream = sc.objectFile[Status]("twitter-*")
    val streamByDate = stream.map(status => (status.getCreatedAt(),status)).sortByKey(true).groupByKey()
    /**
    val firstTenStatus = streamByDate.take(10)
    firstTenStatus.map{case(time,status) => 
        status.map{status => println("%s".format(status.getCreatedAt()))}}
**/
    val rddQueue = new SynchronizedQueue[RDD[Status]]()
    val rddList = streamByDate.collect().map{case(time,status) =>
        sc.parallelize(status.toList)
    }
    rddQueue ++= rddList
    
    val inputStream = ssc.queueStream(rddQueue,true)
    
    val hashTags = inputStream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                          .map{case (topic, count) => (count, topic)}
                          .transform(_.sortByKey(false))
    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
                              .map{case (topic, count) => (count, topic)}
                              .transform(_.sortByKey(false))
    var users = inputStream.map(status => status.getUser.getScreenName)
    val topUsers60 = users.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                              .map{case (topic, count) => (count, topic)}
                              .transform(_.sortByKey(false))
    // Print popular hashtags
    topCounts10.foreachRDD(rdd => {
                 val topList = rdd.take(10)
                 println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
                 topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
               })



    var currTime = streamByDate.first()._1.getTime()

    ssc.start()
    ssc.awaitTermination()
    /**
   val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    .map((_,1)).reduceByKey(_+_).sortByKey(true).map{case(topic,count) => (count,topic)}.sortByKey(false)
   val firstTen = hashTags.take(20)
   firstTen.map{case (count,tag) => println("%s (%s tweets)".format(tag,count))}
**/

  }
}
// scalastyle:on println
