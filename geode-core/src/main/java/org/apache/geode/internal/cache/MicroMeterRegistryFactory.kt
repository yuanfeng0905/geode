package org.apache.geode.internal.cache

import io.micrometer.core.instrument.MeterRegistry

object MicroMeterRegistryFactory {
    private val micrometerStats = MicrometerStats()
    fun getMeterRegistry(): MeterRegistry = micrometerStats.meterRegistry
}