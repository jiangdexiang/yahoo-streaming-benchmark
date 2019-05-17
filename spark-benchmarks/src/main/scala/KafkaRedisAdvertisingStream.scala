/*
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */

// scalastyle:off println

import java.util
import java.util.UUID

import benchmark.common.Utils
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.sedis._
import redis.clients.jedis._

import scala.collection.Iterator
import scala.compat.Platform.currentTime

object KafkaRedisAdvertisingStream {
  def main(args: Array[String]) {

    val commonConfig = Utils.findAndReadConfigFile(args(0), true).asInstanceOf[java.util.Map[String, Any]]
    val batchSize = commonConfig.get("spark.batchtime") match {
      case n: Number => n.longValue()
      case other => throw new ClassCastException(other + " not a Number")
    }
    val topic = commonConfig.get("kafka.topic") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }

    val groupId = commonConfig.get("kafka.group.id") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }

    val redisHost = commonConfig.get("redis.host") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }

    val redisPort = commonConfig.get("redis.port") match {
      case n: Number => n.intValue()
      case other => throw new ClassCastException(other + " not a Number")
    }

    val redisPassword = commonConfig.get("redis.password") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaRedisAdvertisingStream")
    if (isLocal) {
      sparkConf.setMaster("local[2]")
    }
    val ssc = new StreamingContext(sparkConf, Milliseconds(batchSize))

    //    val kafkaHosts = commonConfig.get("kafka.brokers").asInstanceOf[java.util.List[String]] match {
    //      case l: java.util.List[String] => l.asScala
    //      case other => throw new ClassCastException(other + " not a List[String]")
    //    }
    //    val kafkaPort = commonConfig.get("kafka.port") match {
    //      case n: String => n
    //      case other => throw new ClassCastException(other + " not a String")
    //    }

    // Create direct kafka stream with brokers and topics
    //    val topicsSet = Set(topic)
    val brokers = commonConfig.get("kafka.bootstrap.server") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> java.lang.Boolean.FALSE
    )
    System.err.println(
      "Trying to connect to Kafka at " + brokers)
    //    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    //      ssc, kafkaParams, topicsSet)

    val topicsSet = topic.split(",").toSet
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    //We can repartition to use more executors if desired
    //    val messages_repartitioned = messages.repartition(10)


    //take the second tuple of the implicit Tuple2 argument _, by calling the Tuple2 method ._2
    //The first tuple is the key, which we don't use in this benchmark
    val kafkaRawData = messages.map(data => data.value())

    //Parse the String as JSON
    val kafkaData = kafkaRawData.map(parseJson)

    //Filter the records if event type is "view"
    val filteredOnView = kafkaData.filter(data => data(3).equals("bigc_app_ershou"))

    //project the event, basically filter the fileds.
    val projected = filteredOnView.map(eventProjection)

    //Note that the Storm benchmark caches the results from Redis, we don't do that here yet
    val redisJoined = projected.mapPartitions(queryRedisTopLevel(_, redisHost, redisPort, redisPassword), preservePartitioning = false)

    val campaign_timeStamp = redisJoined.map(campaignTime)
    //each record in the RDD: key:(campaign_id : String, window_time: Long),  Value: (ad_id : String)
    //DStream[((String,Long),String)]

    val groupedByCampaignTime = campaign_timeStamp.groupByKey()
    // DStream[((String,Long), Iterable[String])]

    val totalEventsPerCampaignTime = groupedByCampaignTime.map(x => {
      (x._1, x._2.size)
    })
    //DStream[((String,Long), Int)]
    //each record: key:(campaign_id, window_time),  Value: number of events

    //Repartition here if desired to use more or less executors
    //    val totalEventsPerCampaignTime_repartitioned = totalEventsPerCampaignTime.repartition(20)

    val final_results = totalEventsPerCampaignTime.mapPartitions(writeRedisTopLevel(_, redisHost, redisPort, redisPassword), preservePartitioning = false)
    final_results.count().print()

    // Start the computation
    ssc.start
    ssc.awaitTermination
  }

  def isLocal: Boolean = {
    val os = System.getProperty("os.name").toLowerCase
    os.contains("windows") || os.contains("mac os x")
  }


  def joinHosts(hosts: Seq[String], port: String): String = {
    val joined = new StringBuilder()
    hosts.foreach({
      if (joined.nonEmpty) {
        joined.append(",")
      }

      joined.append(_).append(":").append(port)
    })
    joined.toString()
  }

  def parseJson(jsonString: String): Array[String] = {
    val parser = JSON.parseObject(jsonString)
    Array(
      parser.getString("ucid"),
      parser.getString("device_uid"),
      parser.getString("uuid"),
      parser.getString("pid"),
      parser.getString("evt_id"),
      parser.getString("time_local"),
      parser.getString("client_timestamp"))
  }

  def eventProjection(event: Array[String]): Array[String] = {
    Array(
      event(0), //ucid
      event(6)) //time_local
  }

  def queryRedisTopLevel(eventsIterator: Iterator[Array[String]], redisHost: String, redisPort: Int, redisPassword: String): Iterator[Array[String]] = {
    val pool = new Pool(new JedisPool(new JedisPoolConfig(), redisHost, redisPort, 2000, redisPassword))
    val ad2campaign = new util.HashMap[String, String]()
    val eventsIteratorMap = eventsIterator.map(event => queryRedis(pool, ad2campaign, event))
    pool.underlying.getResource.close()
    eventsIteratorMap
  }

  def queryRedis(pool: Pool, ad2campaign: util.HashMap[String, String], event: Array[String]): Array[String] = {
    val ucid = event(0)
    val campaign_id_cache = ad2campaign.get(ucid)
    if (campaign_id_cache == null) {
      pool.withJedisClient { client =>
        //        val campaign_id_temp = Dress.up(client).get(ucid)
        //        if (campaign_id_temp.isDefined) {
        //          val campaign_id = campaign_id_temp.get
        //          ad2campaign.put(ucid, campaign_id)
        //          Array(campaign_id, event(0), event(1))
        //          campaign_id, ad_id, event_time
        //        } else {
        //          Array("Campaign_ID not found in either cache nore Redis for the given ad_id!", event(0), event(1))
        //        }
        val campaign_id = client.get(ucid)
        if (campaign_id != null) {
          ad2campaign.put(ucid, campaign_id)
          Array(campaign_id, event(0), event(1))
          //          campaign_id, ad_id, event_time
        } else {
          Array("Campaign_ID not found in either cache nore Redis for the given ad_id!", event(0), event(1))
        }
      }
    } else {
      Array(campaign_id_cache, event(0), event(1))
    }
  }

  def campaignTime(event: Array[String]): ((String, Long), String) = {
    val time_divisor: Long = 10000L
    ((event(0), time_divisor * (event(2).toLong / time_divisor)), event(1))
    //Key: (campaign_id, window_time),  Value: ad_id
  }

  def writeRedisTopLevel(campaign_window_counts_Iterator: Iterator[((String, Long), Int)], redisHost: String, redisPort: Int, redisPassword: String): Iterator[String] = {
    val pool = new Pool(new JedisPool(new JedisPoolConfig(), redisHost, redisPort, 2000, redisPassword))

    val campaign_window_counts_IteratorMap =
      campaign_window_counts_Iterator.map(campaign_window_counts => writeWindow(pool, campaign_window_counts))
    pool.underlying.getResource.close()
    campaign_window_counts_IteratorMap
  }

  private def writeWindow(pool: Pool, campaign_window_counts: ((String, Long), Int)): String = {
    val campaign_window_pair = campaign_window_counts._1
    val campaign = campaign_window_pair._1
    val window_timestamp = campaign_window_pair._2.toString
    val window_seenCount = campaign_window_counts._2
    pool.withJedisClient { client =>

      val dressUp = Dress.up(client)
      var windowUUID = dressUp.hmget(campaign, window_timestamp).head
      if (windowUUID == null) {
        windowUUID = UUID.randomUUID().toString
        dressUp.hset(campaign, window_timestamp, windowUUID)
        var windowListUUID: String = dressUp.hmget(campaign, "windows").head
        if (windowListUUID == null) {
          windowListUUID = UUID.randomUUID.toString
          dressUp.hset(campaign, "windows", windowListUUID)
        }
        dressUp.lpush(windowListUUID, window_timestamp)
      }
      dressUp.hincrBy(windowUUID, "seen_count", window_seenCount)
      dressUp.hset(windowUUID, "time_updated", currentTime.toString)
      return window_seenCount.toString
    }

  }
}
