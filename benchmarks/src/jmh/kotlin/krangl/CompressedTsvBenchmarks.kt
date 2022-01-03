package krangl

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.io.File
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 2)
open class CompressedTsvBenchmarks {

    val tsvFile = File("src/jmh/resources/nycflights.tsv.gz")

    @Benchmark
    fun readTsv(bh: Blackhole) {
        val df = DataFrame.readTSV(tsvFile).apply {
            check(nrow == 336776)
            check(ncol == 16)
        }
        bh.consume(df)
    }
}
