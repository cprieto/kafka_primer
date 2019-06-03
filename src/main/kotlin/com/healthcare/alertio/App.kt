package com.healthcare.alertio

import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Options
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
class Application

val options = Options()
        .addOption("all", "Run all runners")
        .addOption("healthcheck", "Run only Healthcheck producer")
        .addOption("uptimes", "Run only Uptimes consumer")

val CmdLineParser = DefaultParser()

fun main(args: Array<String>) {
    SpringApplication.run(Application::class.java, *args)
}

fun Array<String>.parse() = CmdLineParser.parse(options, this)
