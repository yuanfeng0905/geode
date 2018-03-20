package org.apache.geode.internal.protocol.protobuf.statistics

import io.micrometer.core.instrument.MeterRegistry
import org.apache.geode.internal.cache.MicroMeterRegistryFactory
import java.util.concurrent.atomic.AtomicInteger

open class MicrometerClientStatsImpl(val meterRegistry: MeterRegistry) : ClientStatistics {
    override fun startOperation(): Long {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun endOperation(startOperationTime: Long) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    constructor() : this(MicroMeterRegistryFactory.getMeterRegistry())

    private val clientsConnected = AtomicInteger(0)

    val clientConnectedCounter = meterRegistry.gauge("clientConnected", clientsConnected)
    val messageReceivedCounter = meterRegistry.summary("messageReceived")
    val messageSentCounter = meterRegistry.summary("messageSent")
    val authorizationViolationsCounter = meterRegistry.counter("authorizationViolations")
    val authenticationFailureCounter = meterRegistry.counter("authenticationFailures")

    override fun clientConnected() {
        clientsConnected.incrementAndGet()
    }

    override fun clientDisconnected() {
        clientsConnected.decrementAndGet()
    }

    override fun messageReceived(bytes: Int) {
        messageReceivedCounter.record(bytes.toDouble())
    }

    override fun messageSent(bytes: Int) {
        messageSentCounter.record(bytes.toDouble())
    }

    override fun incAuthorizationViolations() {
        authorizationViolationsCounter.increment()
    }

    override fun incAuthenticationFailures() {
        authenticationFailureCounter.increment()
    }
}