package krangl.benchmarking

import de.mpicbg.scicomp.kutils.MicroBenchmark
import krangl.DataFrame
import krangl.innerJoin
import krangl.readTSV
import krangl.take

fun main() {
    val flights = DataFrame.readTSV(object {}.javaClass.getResource("/krangl/data/nycflights.tsv.gz").file)
            .take(10000)

    val benchmark = MicroBenchmark<String>(reps = 3, warmupReps = 0)
    benchmark.elapseNano("big join") {
        val bigJoin = flights.innerJoin(flights, by = listOf("dep_delay", "carrier"))
        println("${flights.nrow} flights -> ${bigJoin.nrow} results")
    }
    benchmark.printSummary()
/*
#flights | #results | time (git f5da9c)   | time (git head)
---------+----------+---------------------+--------------------
     100 |      224 | mean=48 sd=30       | mean=47 sd=29
    1000 |     7408 | mean=709 sd=126     | mean=647 sd=113
    5000 |   146444 | mean=29618  sd=219  | mean=27559 sd=469
   10000 |   618854 | mean=164188 sd=3620 | mean=129076 sd=1388
*/
}

