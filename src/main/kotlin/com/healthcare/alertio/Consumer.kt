package com.healthcare.alertio

import com.beust.klaxon.Klaxon
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import java.time.LocalDate
import java.time.Period
import java.time.ZoneId
import java.util.*


@Component
class HealthCheckConsumer(@Value("\${topic.healthcheck}") private val healthcheckTopic: String,
                          @Value("\${topic.uptime}") private val uptimeTopic: String,
                          private val klaxon: Klaxon,
                          private val streamsBuilder: StreamsBuilder) {

    private val log = LoggerFactory.getLogger(HealthCheckConsumer::class.java)

    @Value("\${bootstrap.servers}")
    lateinit var servers: String

    @Value("\${application.id}")
    lateinit var applicationId: String

    fun run() {
        log.info("Transforming healthcheck records")
        val healthCheckStream = streamsBuilder.stream(
                healthcheckTopic, Consumed.with(Serdes.String(), Serdes.String()))

        val input = healthCheckStream.mapValues { value -> klaxon.parse<HealthCheck>(value)!! }
        val output = input.map { _, value ->
            KeyValue(value.serial, "${value.lastStartedAt.uptime().days}")
        }

        output.to(uptimeTopic, Produced.with(Serdes.String(), Serdes.String()))

        val topology = streamsBuilder.build()

        val properties = Properties()
        properties["bootstrap.servers"] = servers
        properties["application.id"] = applicationId

        val streams = KafkaStreams(topology, properties)
        streams.start()
    }
}

fun Date.uptime(): Period {
    val startedAt = this.toInstant().atZone(ZoneId.systemDefault()).toLocalDate()
    return Period.between(startedAt, LocalDate.now())
}

@Component
@Order(value = 2)
class HeathCheckConsumerRunner(private val consumer: HealthCheckConsumer): CommandLineRunner {
    override fun run(vararg args: String?) {
        val opts = CmdLineParser.parse(options, args)
        if(opts.hasOption("all") || opts.hasOption("uptimes")) consumer.run()
    }
}

inline fun catchLog(log: Logger, action: () -> Unit) {
    try {
        action()
    } catch(e: Throwable) {
        log.error(e.message)
    }
}