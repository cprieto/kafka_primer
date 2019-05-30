package com.healthcare.alertio

import com.github.javafaker.Faker
import java.time.LocalDate
import java.util.*
import java.util.concurrent.TimeUnit

@Target(AnnotationTarget.FIELD)
annotation class KlaxonDate

data class HealthCheck(
        val event: String,
        val factory: String,
        val serial: String,
        val type: String,
        val status: String,
        @KlaxonDate val lastStartedAt: Date,
        val temperature: Float,
        val ipAddress: String
) {
    companion object
}




private val faker = Faker()

private val machineStatuses = MACHINE_STATUS.values()
private val machineTypes = MACHINE_TYPE.values()
private val numStatuses = machineStatuses.size - 1
private val numTypes = machineTypes.size - 1

fun HealthCheck.Companion.random() = HealthCheck(
    HEALTH_CHECK,
        faker.address().city(),
        faker.bothify("??##-??##", true),
        "${machineTypes[faker.number().numberBetween(0, numTypes)]}",
        "${machineTypes[faker.number().numberBetween(0, numStatuses)]}",
        faker.date().past(100, TimeUnit.DAYS),
        faker.number().numberBetween(100L, 0L).toFloat(),
        faker.internet().ipV4Address()
)