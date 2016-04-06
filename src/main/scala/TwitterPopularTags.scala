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

import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging
import org.apache.spark.rdd._

/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 * Run this on your local machine as
 *
 */
object StreamingExamples extends Logging{
    def setStreamingLogLevels(){
        val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
        if(!log4jInitialized){
            logInfo("Setting log level to [WARN] for streaming example.")
            Logger.getRootLogger.setLevel(Level.OFF)
        }
    }
}

object TwitterPopularTags {
  def main(args: Array[String]) {

    val consumerKey = TwitterCred.consumerKey
    val consumerSecret = TwitterCred.consumerSecret
    val accessToken = TwitterCred.accessToken
    val accessTokenSecret = TwitterCred.accessTokenSecret

    StreamingExamples.setStreamingLogLevels()

    val filters = Array()

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf()
        .setAppName("TwitterPopularTags")
        .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("./checkpoint")
    val stream = TwitterUtils.createStream(ssc, None)



    // Starts with 30 seconds windows

    //Volume
    
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    
    val meanVolume  = hashTags.map((_,1)).reduceByKeyAndWindow(_ + _, Seconds(30))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))

    //Velocity

//    val hashTagsTime = stream.flatMap( statuc => status.getText.split(" ").filter(_.) )
                        


//Unique User

    val hashTagsUser = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")).map{x => ( x, status.getUser.getScreenName)})
    
    val usersPerHashTags = hashTagsUser.mapValues[List[String]]{ user =>  List(user)}
                                        .reduceByKey{case (user1,user2) => List.concat(user1,user2)}
                                        .mapValues{ userList => userList.distinct.length} //Temporarily without distinct
                                        .reduceByKeyAndWindow(_ + _, Seconds(30))
                                        .map{case (hash,count) => (count,hash)}
                                        .transform(_.sortByKey(false))
//                                        .map{case(hash,user) =>  (hash,user.split(" ").toList.distinct().count() )}
    

//MultiTag Activeness
    
    val multiHashTags = stream.map{status => status.getText.split(" ").filter(_.startsWith("#"))}

    val windowActiveness = multiHashTags.flatMap{ hashs => hashs.map(x => (x,hashs.length)) }
                                        .reduceByKeyAndWindow(_ + _, Seconds(30))
                                        .map{case (topic, count) => (count, topic)}
                                         .transform(_.sortByKey(false))


//1stDeg Count

/**
    val firstDegCount = multiHashTags.flatMap(x => x.map( (_,List(x))))
                                        .reduceByKey(_+_) //Compute all other hash in window
                                        .map{case (hash,otherhashs) => (hash,otherhashs.distinct())} //Unique Hashs
                                        .map{case (hash,otherhashs) => (hash, otherhashs)    }
**/
    
    /**multiHashTags.transform[(String,Int)]{ (rdd:RDD[List[String]],t:Time) => rdd.map{ x => x.flatMap{ hash =>
                                                    (hash, x.flatMap{otherhash => meanVolume.compute(t).get.map{case(count,hash)=>(hash,count)}.lookup(hash)}.reduce(_+_))
                                                } } } **/


/**  Test Environment
    firstDegCount.foreachRDD (rdd => {
        val topList = rdd.take(10)
        println("\n============Test test Activeness ===============")
        topList.foreach{println(_)}
 //       topList.foreach{ case(key,value)=> println("%s %s".format(key,value)) }
    })
    **/



    meanVolume.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nTop Volume Hashtags in past 30 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })


    usersPerHashTags.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nTop Unique User Count Hashtags in past 30 seconds:")
      topList.foreach{case (count, tag) => println("%s (%s Unique Users)".format(tag, count))}
    })

    windowActiveness.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nTop Active (Chain)Hashtags in past 30 seconds:")
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

   


    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
