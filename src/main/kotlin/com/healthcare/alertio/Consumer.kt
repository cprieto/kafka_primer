package com.healthcare.alertio

import com.beust.klaxon.Klaxon
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@Component
class HealthCheckConsumer(@Value("\${topic.healthcheck}") private val healthcheckTopic: String,
                          private val klaxon: Klaxon,
                          private val consumer: KafkaConsumer<String, String>,
                          private val reporter: UptimeReporter) {

    private val log = LoggerFactory.getLogger(HealthCheckConsumer::class.java)
    private val executor = Executors.newSingleThreadExecutor()

    fun run() {
        log.info("Transforming healthcheck records")
        consumer.subscribe(listOf(healthcheckTopic))
        executor.doForever {
            val records = consumer.poll(Duration.ofSeconds(1L))
            records.forEach { transformRecords(it) }
        }
    }

    fun transformRecords(record: ConsumerRecord<String, String>) {
        catchLog(log) {
            val healthcheck = klaxon.parse<HealthCheck>(record.value())!!
            reporter.reportHealthcheck(healthcheck)
        }
    }
}

fun ExecutorService.doForever(action: () -> Unit) {
    while(true) {
        action()
    }
}

@Component
@Order(value = 2)
class HeathCheckConsumerCommandRunner(private val consumer: HealthCheckConsumer): CommandLineRunner {
    override fun run(vararg args: String?) {
        consumer.run()
    }
}

inline fun catchLog(log: Logger, action: () -> Unit) {
    try {
        action()
    } catch(e: Throwable) {
        log.error(e.message)
    }
}