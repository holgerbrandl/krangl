package krangl.benchmarking

import krangl.format
import krangl.mean
import krangl.sd

@Deprecated("use MicroBenchmark instead")
data class RunTimes<T>(val result: T, val runtimes: List<Double>) {
    val mean by lazy { runtimes.toTypedArray().mean() }

    override fun toString(): String {
        // todo use actual confidence interval here
        return "${mean.format(2)} ± ${runtimes.toTypedArray().sd()?.format(2)} SD, N=${runtimes.size} "
    }

    companion object {
        inline fun <R> measure(block: () -> R, numRuns: Int = 1, warmUp: Int = 0): RunTimes<R> {

            require(numRuns > 0)

            var result: R? = null

            val runs = (1..(numRuns + warmUp)).map {
                val start = System.currentTimeMillis()
                result = block()
                (System.currentTimeMillis() - start) / 1000.toDouble()
            }

            return RunTimes<R>(result!!, runs.drop(warmUp))
        }

    }
}