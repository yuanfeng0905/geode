package org.apache.geode.internal.protocol.protobuf.statistics

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import org.apache.geode.internal.cache.CachePerfStats
import org.apache.geode.internal.cache.MicroMeterRegistryFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class TimedMicrometerClientStatsImpl(meterRegistry: MeterRegistry) : MicrometerClientStatsImpl(meterRegistry) {

    constructor() : this(MicroMeterRegistryFactory.getMeterRegistry())

    private fun constructTimerForMetric(metricName: String): Timer =
            meterRegistry.timer("${metricName}Latency", emptyList())

    private val operationTimer = constructTimerForMetric("operation")

    override fun startOperation(): Long = CachePerfStats.getStatTime()

    override fun endOperation(startOperationTime: Long) {
        updateTimer(startOperationTime, operationTimer)
    }

    private fun updateTimer(startTimeInNanos: Long, timer: Timer) {
        timer.record((System.nanoTime() - startTimeInNanos), TimeUnit.NANOSECONDS)
    }
}