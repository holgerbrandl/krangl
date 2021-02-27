package krangl.benchmarking

import krangl.*
import krangl.util.MicroBenchmark
import org.apache.commons.csv.CSVFormat
import java.io.*
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPInputStream


internal fun DataFrame.Companion.fromCSVlistArray(reader: Reader, format: CSVFormat = CSVFormat.DEFAULT): DataFrame {
    val csvParser = format.withFirstRecordAsHeader().parse(reader)

    val rawCols = mutableMapOf<String, List<String?>>()
    val records = csvParser.records

    for (colName in csvParser.headerMap.keys) {
        rawCols.put(colName, records.map { it[colName].naAsNull() })
    }

    // parallelize this for better performance
//    val colModel = rawCols.toList().pmap { rawCol ->
    val colModel = rawCols.map { rawCol ->
        val colData = rawCol.value
        val colName = rawCol.key

        val firstElements = colData.take(5)

        when {
            isIntCol(firstElements) -> IntCol(colName, colData.map { it?.toInt() })
            isLongCol(firstElements) -> LongCol(colName, colData.map { it?.toLong() })
            isDoubleCol(firstElements) -> DoubleCol(colName, colData.map { it?.toDouble() })
            isBoolCol(firstElements) -> BooleanCol(colName, colData.map { it?.cellValueAsBoolean() })
            else -> StringCol(colName, colData.map { it })
        }
    }

    return SimpleDataFrame(colModel)
}


internal fun DataFrame.Companion.fromCSVArray(reader: Reader, format: CSVFormat = CSVFormat.DEFAULT): DataFrame {
    val csvParser = format.withFirstRecordAsHeader().parse(reader)

    val rawCols = mutableMapOf<String, Array<String?>>()
    val records = csvParser.records

    // first fill guess buffer and then
    for (colName in csvParser.headerMap.keys) {
        rawCols.put(colName, Array(records.size, { records[it][colName].naAsNull() }))
    }


    // parallelize this for better performance
//    val colModel = rawCols.toList().pmap { rawCol ->
//
//        val colName = rawCol.first
//        val colData = rawCol.second
//
//        val firstElements = colData.take(5)
    val colModel = rawCols.map { rawCol ->
        val colData = rawCol.value
        val colName = rawCol.key

        val firstElements = rawCol.value.take(5)

        when {
            isIntCol(firstElements) -> IntCol(colName, colData.map { it?.toInt() })
            isLongCol(firstElements) -> LongCol(colName, colData.map { it?.toLong() })
            isDoubleCol(firstElements) -> DoubleCol(colName, colData.map { it?.toDouble() })
            isBoolCol(firstElements) -> BooleanCol(colName, colData.map { it.cellValueAsBoolean() })
            else -> StringCol(colName, colData.map { it })
        }
    }

    return SimpleDataFrame(colModel)
}

fun <T, R> Iterable<T>.pmap(
    numThreads: Int = Runtime.getRuntime().availableProcessors() - 2,
    exec: ExecutorService = Executors.newFixedThreadPool(numThreads),
    transform: (T) -> R
): List<R> {

    // default size is just an inlined version of kotlin.collections.collectionSizeOrDefault
    val defaultSize = if (this is Collection<*>) this.size else 10
    val destination = Collections.synchronizedList(ArrayList<R>(defaultSize))

    for (item in this) {
        exec.submit { destination.add(transform(item)) }
    }

    exec.shutdown()
    exec.awaitTermination(1, TimeUnit.DAYS)

    return ArrayList<R>(destination)
}

internal fun main(args: Array<String>) {

    fun getFlightsReader(): BufferedReader {
        val flightsFile = File("/Users/brandl/projects/kotlin/krangl/src/test/resources/krangl/data/nycflights.tsv.gz")
        val gzip = GZIPInputStream(FileInputStream(flightsFile));
        return BufferedReader(InputStreamReader(gzip));
    }


    //make sure that the results are identical
    //    DataFrame.readTsvUnivox(getFlightsReader()).schema()
    DataFrame.readDelim(getFlightsReader(), CSVFormat.TDF.withHeader()).schema()


    val mb = MicroBenchmark<String>(reps = 20, warmupReps = 5)

    mb.elapseNano("apache_commons") {
        DataFrame.readDelim(getFlightsReader(), CSVFormat.TDF)
    }

    //    mb.elapseNano("univocity") {
    //        DataFrame.readTsvUnivox(getFlightsReader())
    //    }

    //    mb.results.first().mean
    mb.asDataFrame().print()
}

fun <T> MicroBenchmark<T>.asDataFrame(): DataFrame {
    //    return results.deparseRecords (
    //        "config" with { config },
    //        "num_reps" with { reps },
    //        "" with { times }
    //    )

    return results.asDataFrame()
}


