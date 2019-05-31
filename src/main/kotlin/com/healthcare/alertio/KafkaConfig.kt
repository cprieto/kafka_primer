package com.healthcare.alertio

import com.beust.klaxon.Converter
import com.beust.klaxon.JsonValue
import com.beust.klaxon.Klaxon
import com.beust.klaxon.KlaxonException
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.text.SimpleDateFormat
import java.util.*

val dateConverter = object: Converter {
    private val parser = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

    init {
        parser.timeZone = TimeZone.getDefault()
    }

    override fun canConvert(cls: Class<*>) = cls == Date::class.java

    override fun fromJson(jv: JsonValue) =
        if (jv.string != null) {
            parser.parse(jv.string)
        } else { throw KlaxonException("Couldn't parse date ${jv.string}")}


    override fun toJson(value: Any) = """ "${parser.format(value)}" """
}

@Configuration
class KafkaConfigurator {
    @Value("\${bootstrap.servers}")
    lateinit var servers: String

    @Bean
    fun kafkaProducer(): KafkaProducer<String, String> {
        val properties = Properties()
        properties["bootstrap.servers"] = servers
        properties["key.serializer"] = StringSerializer::class.java
        properties["value.serializer"] = StringSerializer::class.java

        return KafkaProducer(properties)
    }

    @Bean
    fun streamsBuilder() = StreamsBuilder()

    @Bean
    fun klaxon(): Klaxon = Klaxon().fieldConverter(KlaxonDate::class, dateConverter)
}

@Component
class KafkaTopicStarter(private val streamsBuilder: StreamsBuilder) {


    @EventListener(ContextRefreshedEvent::class)
    fun start() {

    }
}
