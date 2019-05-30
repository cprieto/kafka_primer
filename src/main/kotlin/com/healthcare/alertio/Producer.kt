package com.healthcare.alertio

import com.beust.klaxon.Klaxon
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@Component
@Order(value = 1)
class ProducerCommandRunner(private val producer: PlainTextProducer): CommandLineRunner {
    override fun run(vararg args: String?) {
        producer.run()
    }
}

@Component
class PlainTextProducer(
        @Value("\${producer.rate}") private val ratePerSecond: Int,
        @Value("\${topic.healthcheck}") private val healthcheckTopic: String,
        private val klaxon: Klaxon,
        private val producer: KafkaProducer<String, String>) {

    private val log = LoggerFactory.getLogger(PlainTextProducer::class.java)
    private val executor = Executors.newSingleThreadScheduledExecutor()


    fun run() {
        val period = 1000L / ratePerSecond
        log.info("Starting producer at rate $period ms")
        executor.scheduleAtFixedRate({
            catchLog(log) {
                val data = HealthCheck.random()
                val record = ProducerRecord<String, String>(healthcheckTopic, klaxon.toJsonString(data))
                producer.send(record)
            }
        }, 0, period, TimeUnit.MILLISECONDS)
    }
}
