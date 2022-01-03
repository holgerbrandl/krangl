package krangl

import org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.TimeUnit
import kotlin.random.Random


@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 3)
open class DataframeBenchmarks {

    @State(Scope.Benchmark)
    open class ExecutionPlan {

        @Param("1_000", "10_000", "100_000", "1_000_000", "10_000_000")
        lateinit var size: String

        lateinit var doubleData: DoubleArray
        lateinit var booleanData: Array<Boolean?>
        lateinit var intData: IntArray
        lateinit var longData: LongArray
        lateinit var stringData: Array<String?>


        @Setup(Level.Trial)
        fun setUp() {
            val intSize = size.replace("_", "").toInt()

            doubleData = DoubleArray(intSize) { Math.random() }
            booleanData = Array(intSize) { Random.nextBoolean() }
            intData = IntArray(intSize) { Random.nextInt() }
            longData = LongArray(intSize) { Random.nextLong() }
            stringData = Array(intSize) { randomAlphanumeric(5, 30) }
            println("### Completed setUp() for: $size ###")
        }
    }


    @Benchmark
    fun dataFrameOf(plan: ExecutionPlan, bh: Blackhole) {
        val df = dataFrameOf(
            DoubleCol("doubleData", plan.doubleData),
            BooleanCol("booleanData", plan.booleanData),
            IntCol("intData", plan.intData),
            LongCol("longData", plan.longData),
            StringCol("stringData", plan.stringData)
        )

        bh.consume(df)
    }
}
