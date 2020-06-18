package krangl.benchmarking

import de.mpicbg.scicomp.kutils.MicroBenchmark
import krangl.DataFrame
import krangl.innerJoin
import krangl.readTSV
import krangl.take

fun main() {
    val flights = DataFrame.readTSV(object {}.javaClass.getResource("/krangl/data/nycflights.tsv.gz").file)
            .take(5000)

    val benchmark = MicroBenchmark<String>(reps = 3, warmupReps = 0)
    benchmark.elapseNano("big join") {
        val bigJoin = flights.innerJoin(flights, by = listOf("dep_delay", "carrier"))
        println("${flights.nrow} flights -> ${bigJoin.nrow} results")
    }
    benchmark.printSummary()
/*
#reps | #flights | #results | time (git f5da9c2)  | time (git 958efb5)  | time (git head)
------+----------+----------+---------------------+---------------------+-------------------
 1000 |      100 |      224 | mean=9.4 sd=3.9     | mean=9.1 sd=3.4     | mean=5.6 sd=2.4
  100 |     1000 |     7408 | mean=539 sd=39      | mean=432 sd=40      | mean=30 sd=14
    3 |     5000 |   146444 | mean=29618 sd=219   | mean=27559 sd=469   | mean=213 sd=89
    3 |    10000 |   618854 | mean=164188 sd=3620 | mean=129076 sd=1388 | mean=454 sd=129
    3 |    50000 | 14305392 | >4h/rep? (gave up)  | did not try         | mean=19787 sd=8689
*/
}
