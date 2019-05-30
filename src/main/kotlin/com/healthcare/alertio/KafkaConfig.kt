package com.healthcare.alertio

import com.beust.klaxon.Converter
import com.beust.klaxon.JsonValue
import com.beust.klaxon.Klaxon
import com.beust.klaxon.KlaxonException
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Scope
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

    @Value("\${group.id}")
    lateinit var groupId: String

    @Bean
    fun kafkaProducer(): KafkaProducer<String, String> {
        val properties = Properties()
        properties["bootstrap.servers"] = servers
        properties["key.serializer"] = StringSerializer::class.java
        properties["value.serializer"] = StringSerializer::class.java

        return KafkaProducer(properties)
    }

    @Bean
    fun kafkaConsumer(): KafkaConsumer<String, String> {
        val properties = Properties()
        properties["bootstrap.servers"] = servers
        properties["key.deserializer"] = StringDeserializer::class.java
        properties["value.deserializer"] = StringDeserializer::class.java
        properties["group.id"] = groupId

        return KafkaConsumer(properties)
    }

    @Bean
    fun klaxon(): Klaxon = Klaxon().fieldConverter(KlaxonDate::class, dateConverter)
}
