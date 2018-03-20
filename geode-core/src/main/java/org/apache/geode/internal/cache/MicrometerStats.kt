package org.apache.geode.internal.cache

import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.MeterBinder
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics
import io.micrometer.core.instrument.composite.CompositeMeterRegistry

import io.micrometer.influx.InfluxConfig
import io.micrometer.influx.InfluxMeterRegistry
import io.micrometer.jmx.JmxConfig
import io.micrometer.jmx.JmxMeterRegistry
import io.pivotal.gemfire.micrometer.binder.LoadAvgMetrics
import io.pivotal.gemfire.micrometer.binder.MemInfoMetrics
import io.pivotal.gemfire.micrometer.binder.StatMetrics
import io.pivotal.gemfire.micrometer.procOS.ProcOSLoadAvg
import io.pivotal.gemfire.micrometer.procOS.ProcOSMemInfo
import io.pivotal.gemfire.micrometer.procOS.ProcOSReaderFactory
import io.pivotal.gemfire.micrometer.procOS.ProcOSStat
import org.apache.geode.cache.CacheFactory
import java.time.Duration
import java.util.ArrayList

class MicrometerStats {
    private val registeredMetricsBinders = ArrayList<MeterBinder>()
    val meterRegistry = CompositeMeterRegistry(Clock.SYSTEM)
    private val influxMetrics: MeterRegistry = InfluxMeterRegistry(object : InfluxConfig {
        override fun step(): Duration = Duration.ofSeconds(10)
        override fun db(): String = "mydb"
        override fun get(k: String): String? = null
        override fun uri(): String = "http://localhost:8086"
    }, Clock.SYSTEM)

//    private val atlasMetrics: MeterRegistry = AtlasMeterRegistry(object : AtlasConfig {
//        override fun get(k: String?): String? = null
//        override fun enabled(): Boolean = true
//        override fun uri(): String = "http://localhost:7101/api/v1/publish"
//        override fun step(): Duration = Duration.ofSeconds(10)
//    }, Clock.SYSTEM)

    private val jmxMetrics: MeterRegistry = JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM)

    init {
        meterRegistry.add(influxMetrics)
//        meterRegistry.add(atlasMetrics)
        meterRegistry.add(jmxMetrics)

        val procOSReaderFactory = ProcOSReaderFactory()
        registeredMetricsBinders.add(LoadAvgMetrics(procOSLoadAvg = ProcOSLoadAvg(procOSReaderFactory.getInstance(LoadAvgMetrics.LOADAVG))))
        registeredMetricsBinders.add(MemInfoMetrics(procOSMemInfo = ProcOSMemInfo(procOSReaderFactory.getInstance(MemInfoMetrics.MEMINFO))))
        registeredMetricsBinders.add(StatMetrics(procOSStat = ProcOSStat(procOSReaderFactory.getInstance(StatMetrics.STAT))))


        registeredMetricsBinders.add(JvmGcMetrics())
        registeredMetricsBinders.add(JvmMemoryMetrics())
        registeredMetricsBinders.add(JvmThreadMetrics())
        registeredMetricsBinders.add(FileDescriptorMetrics())

        val cache = CacheFactory.getAnyInstance() as InternalCache
        ExecutorServiceMetrics.monitor(meterRegistry,cache.distributionManager.waitingThreadPool,"WaitingThreadPool")

        registeredMetricsBinders.forEach { it.bindTo(meterRegistry) }
    }
}