package com.healthcare.alertio

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.LocalDate
import java.time.Period
import java.time.ZoneId

@Component
class UptimeReporter(@Value("\${topic.uptime}") private val uptimeTopic: String,
                     private val producer: KafkaProducer<String, String>) {
    private val log = LoggerFactory.getLogger(UptimeReporter::class.java)

    fun reportHealthcheck(healthCheck: HealthCheck) {
        val startedAt = healthCheck.lastStartedAt.toInstant().atZone(ZoneId.systemDefault()).toLocalDate()
        val uptime = Period.between(startedAt, LocalDate.now())
        val record = ProducerRecord<String, String>(uptimeTopic, healthCheck.serial, "${uptime.days}")
        producer.send(record).get()
    }
}