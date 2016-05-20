package kplyr.benchmarking

import kplyr.*
import mean
import sd
import kotlin.system.measureTimeMillis

/**

Get data with
Rscript -e '
library(nycflights13)
require(readr)
write_tsv(flights, "nycflights.tsv")
'

cat nycflights.tsv | gzip -c > nycflights.tsv.gz

interactive kotlin repl
kotlinc -J"-Xmx8g" -cp /Users/brandl/projects/kotlin/kplyr/build/classes/main:/Users/brandl/.gradle/caches/modules-2/files-2.1/org.apache.commons/commons-csv/1.3/aeed8320e1b86b27e0b477a898eb7dd049526963/commons-csv-1.3.jar

 */

internal data class RunTimes(val runtimes: List<Float>) {
    val mean by lazy { runtimes.mean() }

    override fun toString(): String {
        // todo use actual confidence interval here
        return "$mean ± ${runtimes.sd()} SD\t "
    }

    companion object {
        public inline fun <R> measure(block: () -> R, numRuns: Int = 1): Pair<R, RunTimes> {
            require(numRuns > 0)

            var result: R? = null

            val runs = (1..numRuns).map {
                val start = System.currentTimeMillis()
                result = block()
                (System.currentTimeMillis() - start) / 1000.toFloat()
            }.let { RunTimes(it) }

            return result!! to runs
        }

    }
}



fun main(args: Array<String>) {
    // todo use/create measureTimeMillis-version that return expression value for simplified workflow

    val flights = RunTimes.measure({ DataFrame.fromTSV("/Users/brandl/Desktop/nycflights.tsv") }, 10).apply {
        println(second)
    }.first

    println ("done reading starting glimpsing")

    flights.names

    flights.glimpse()


    // fixme and add unit tests for empty (row and column-empty) data-frames
    SimpleDataFrame().print()

    var groupedFlights: DataFrame = SimpleDataFrame()

    println("group flights took " + measureTimeMillis {
        groupedFlights = flights.groupBy("carrier")
    })

    println("summarizing carriers took " + measureTimeMillis {
        groupedFlights.summarize(
                "mean_arr_delay" to { it["arr_delay"].mean() }
        ).print()
    })
}