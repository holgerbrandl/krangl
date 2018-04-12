package org.krangl.performance

import krangl.flightsData
import krangl.print
import org.openjdk.jmh.annotations.*

@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
open class KranglBenchmark {

    var value: Double = 0.0

    @Setup
    fun setUp(): Unit {
        value = 3.0
    }

    @Benchmark
    fun flightsGrouping() {
        flightsData.groupBy("tailnum").groupedBy().print()
    }

}