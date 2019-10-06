package com.arquivei.kafkagenericeventparser

import com.arquivei.core.io.streaming.KafkaConfig
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.values.KV
import org.apache.kafka.common.serialization.StringDeserializer
import org.joda.time.Duration

class KafkaRead(sc: ScioContext, kafkaConfig: KafkaConfig) {
  def read(): SCollection[KV[String, String]] =
    sc.customInput("Read From Kafka",
      KafkaIO.read[String, String]()
        .withTopics(kafkaConfig.topics)
        .withKeyDeserializer(classOf[StringDeserializer])
        .withValueDeserializer(classOf[StringDeserializer])
        .withConsumerConfigUpdates(kafkaConfig.properties)
        .commitOffsetsInFinalize()
        .withCreateTime(Duration.standardMinutes(1))
        .withoutMetadata()
    )
}
