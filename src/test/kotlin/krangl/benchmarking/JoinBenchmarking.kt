package krangl.benchmarking

import krangl.DataFrame
import krangl.UnequalByHelpers.innerJoin
import krangl.readTSV
import krangl.take
import java.io.File


fun main(args: Array<String>) {
    val flights = DataFrame.readTSV(File("/Users/brandl/projects/kotlin/krangl/src/test/resources/krangl/data/nycflights.tsv.gz"))


    RunTimes.measure({

        val flights50k = flights.take(5000)
        val bigJoin = flights50k.innerJoin(flights50k, by = listOf("dep_delay" to "dep_delay", "carrier" to "carrier"))
        println(bigJoin.nrow)

        // expected 50k result: 14305392

    }, numRuns = 1, warmUp = 0).apply {
        runtimes
        println(this)
    }
}
