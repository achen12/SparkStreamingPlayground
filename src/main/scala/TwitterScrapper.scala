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

import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging


/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 * Run this on your local machine as
 *
 */

object TwitterScrapper {
  def main(args: Array[String]) {
    var fileDir = "twitter"
    if (args.length > 0){
        System.out.println(args)
        fileDir = args.take(1)(0)
    }
    val consumerKey = TwitterCred.consumerKey
    val consumerSecret = TwitterCred.consumerSecret
    val accessToken = TwitterCred.accessToken
    val accessTokenSecret = TwitterCred.accessTokenSecret

    StreamingExamples.setStreamingLogLevels()


    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf()
        .setAppName("TwitterScrapper")
        .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Minutes(10))
    val stream = TwitterUtils.createStream(ssc, None)
    val streamCount = stream.count()
    stream.foreachRDD{ x => {  
        val count = x.count()
        println("Receiving: %s tweets per minutes".format(count/10))}
        }
    stream.saveAsObjectFiles(fileDir)
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
