package krangl.util

/**
 * @author Holger Brandl
 */

class MicroBenchmark<T>(var reps:Int = 25, var warmupReps:Int = 10) {


    val results = emptyList<BenchmarkResult<T>>().toMutableList()

    // inspired from https://gist.github.com/maxpert/6ca20fe1a70ccf6ef3a5
    inline fun elapseNano(desc: T? = null, callback: () -> Unit): BenchmarkResult<T> {
        (1..warmupReps).forEach { callback() }

        return (1..reps).map {
            print(".")
            val start = System.nanoTime()
            callback()
            System.nanoTime() - start
        }.let { BenchmarkResult(it, desc) }.also {
            results.add(it)
            println()
        }

    }

    fun printSummary() {
        for (result in results) {
            result.printSummary()
        }
    }
}


internal fun List<Number>.mean(): Double = map { it.toDouble() }.sum() / size

internal fun List<Number>.sd() = if (size == 1) null else Math.sqrt(map { Math.pow(it.toDouble() - mean(), 2.toDouble()) }.sum() / size.toDouble())


data class BenchmarkResult<T>(val times: List<Long>, val config: T?) {
    fun printSummary() {
        println(getSummary())
    }

    fun getSummary() = "${config}:\t  mean=${mean}\t  sd=${sd}"

    val timesMS:List<Double>
        get() = times.map { it.toDouble()/1E6 }

    val mean: Double
        get() = timesMS.mean()

    val sd: Double
        get() = timesMS.sd() ?: Double.NaN
}

fun main(args: Array<String>) {


    val mb = MicroBenchmark<String>(reps = 10)

    // run config a
    mb.elapseNano("config a") {
        2 + 2
    }

    mb.elapseNano("config b") {
        2 + 2
    }

    mb.printSummary()


}