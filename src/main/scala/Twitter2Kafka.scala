package streamingtests.twitter


import java.util.HashMap
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging



object Twitter2Kafka {
    def main(args: Array[String]){
        val consumerKey = TwitterCred.consumerKey
        val consumerSecret = TwitterCred.consumerSecret
        val accessToken = TwitterCred.accessToken
        val accessTokenSecret = TwitterCred.accessTokenSecret

        StreamingExamples.setStreamingLogLevels()

        val filters = Array("Clinton")
        // Set the system properties so that Twitter4j library used by twitter stream
        // can use them to generat OAuth credentials
        System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
        System.setProperty("twitter4j.oauth.accessToken", accessToken)
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
        val sparkConf = new SparkConf()
            .setAppName("Twitter2Kafka")
            .setMaster("local[2]")
        val ssc = new StreamingContext(sparkConf, Seconds(1))
        val stream = TwitterUtils.createStream(ssc, None,filters)
        val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
        val topCounts = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(1))
            .map{case (topic, count) => (count, topic)}
            .transform(_.sortByKey(false))


        
        val brokers = "localhost:9092"
        val topic = "Hashtag"

        val props = new HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](props)
        topCounts.foreachRDD(rdd =>{
            val topList = rdd.take(3)
            topList.foreach{case (count,tag) =>
                val message = new ProducerRecord[String,String](topic, null, tag)
                producer.send(message)
            }
            }
        )

        topCounts.foreachRDD(rdd => {
        val topList = rdd.take(3)
            println("\nPopular Hash Votes in last 1 seconds (%s total):".format(rdd.count()))
        })
        // Actual code begin,  above code are from examples in Spark
        ssc.start()
        ssc.awaitTermination()
    }
}
