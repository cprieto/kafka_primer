package com.healthcare.alertio

const val HEALTH_CHECK = "HEALTH_CHECK"

enum class MACHINE_TYPE {
    GEOTHERMAL,
    HYDROELECTRIC,
    NUCLEAR,
    WIND,
    SOLAR
}

enum class MACHINE_STATUS {
    STARTING,
    RUNNING,
    SHUTTING_DOWN,
    SHUT_DOWN
}