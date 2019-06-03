package com.healthcare.alertio

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.stereotype.Component
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.kstream.Serialized
import org.apache.commons.lang3.CharSetUtils.count
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.Windowed
import org.springframework.core.annotation.Order
import java.util.*


@Component
class EventsProducer(
        @Value("\${topic.events}") private val eventsTopic: String,
        private val producer: KafkaProducer<String, String>) {

    private val log = LoggerFactory.getLogger(EventsProducer::class.java)
    private val executor = Executors.newSingleThreadScheduledExecutor()

    fun run() {
        log.info("Producing non regular events")
        executor.scheduleAtFixedRate({
            catchLog(log) {
                val current = System.currentTimeMillis()
                val second = Math.floorMod(current / 1000, 60)
                when {
                    second == 6L -> send(54, current - 12000, "late")
                    second != 54L -> send(second, current, "on time")
                }
            }
        }, 0, 1000, TimeUnit.MILLISECONDS)
    }

    fun send(second: Long, current: Long, message: String) {
        val window = current / 10000 * 10000
        val value = "$window, $second, $message"
        val record = ProducerRecord<String, String>(eventsTopic, null, "$current", value)
        producer.send(record).get()
    }
}

@Component
class EventsConsumer(
        @Value("\${topic.events}") private val eventsTopic: String,
        @Value("\${topic.aggregate}") private val aggregateTopic: String,
        private val streamsBuilder: StreamsBuilder
) {
    private val log = LoggerFactory.getLogger(EventsProducer::class.java)

    @Value("\${bootstrap.servers}")
    lateinit var servers: String

    @Value("\${application.id}")
    lateinit var applicationId: String

    fun run() {
        log.info("Consuming weird events")
        val stream = streamsBuilder.stream(eventsTopic, Consumed.with(Serdes.String(), Serdes.String()))
        stream
                .groupBy({ _, _ -> "foo" }, Serialized.with(Serdes.String(),
                        Serdes.String()))
                .windowedBy(TimeWindows.of(10000L))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .map { ws, i -> KeyValue("${ws.window().start()}", "$i") }
                .to(aggregateTopic, Produced.with(Serdes.String(), Serdes.String()))

        val topology = streamsBuilder.build()

        val properties = Properties()
        properties["bootstrap.servers"] = servers
        properties["application.id"] = applicationId
        properties["auto.offset.reset"] = "latest"
        properties["commit.interval.ms"] = 30000

        val streams = KafkaStreams(topology, properties)
        streams.start()
    }
}

@Component
@Order(value = 3)
class EventsCommandLineRunner(private val producer: EventsProducer) : CommandLineRunner {
    override fun run(vararg args: String?) {
        val opts = CmdLineParser.parse(options, args)
        if (opts.hasOption("all") || opts.hasOption("events")) producer.run()
    }

}

@Component
@Order(value = 4)
class EventsConsumerLineRunner(private val consumer: EventsConsumer): CommandLineRunner {
    override fun run(vararg args: String?) {
        val opts = CmdLineParser.parse(options, args)
        if (opts.hasOption("all") || opts.hasOption("events")) consumer.run()
    }
}