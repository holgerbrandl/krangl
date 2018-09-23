package krangl.benchmarking

import de.mpicbg.scicomp.kutils.MicroBenchmark
import krangl.*
import krangl.experimental.readTsvUnivox
import kravis.*
import org.apache.commons.csv.CSVFormat
import tech.tablesaw.api.ColumnType
import tech.tablesaw.api.ColumnType.STRING
import tech.tablesaw.api.Table
import tech.tablesaw.io.csv.CsvReadOptions
import java.io.*
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPInputStream
import kotlin.math.roundToInt


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
    transform: (T) -> R): List<R> {

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


object BenchmarkCustomers {
    @JvmStatic
    fun main(args: Array<String>) {

        val numRecords = 5E7.toInt()
        val customerDb = customerDb(numRecords = numRecords)

        val mb = MicroBenchmark<String>(reps = 10, warmupReps = 3)

        println("Starting apache_commons Benchmark")

        mb.elapseNano("apache_commons") {

            val format = CSVFormat.DEFAULT.withHeader()
            //            val types = mapOf(
            //                "customer_id" to ColType.String,
            //                "product_id" to ColType.String,
            //                "date_epoch" to ColType.String,
            //                "transaction_amount" to ColType.String,
            //                "merchant_id" to ColType.String
            //            )

            val df = DataFrame.readTSV(customerDb, format).apply { println(nrow) }
        }

        println("Starting univocity Benchmark")


        mb.elapseNano("univocity") {

            //            val types = mapOf(
            //                "customer_id" to ColType.String,
            //                "product_id" to ColType.String,
            //                "date_epoch" to ColType.String,
            //                "transaction_amount" to ColType.String,
            //                "merchant_id" to ColType.String
            //            )

            val df = DataFrame.readTsvUnivox(FileReader(customerDb)).apply { println(nrow) }
        }

        println("Starting tablesaw Benchmark")

        mb.elapseNano("tablesaw") {
            //            val types = arrayOf(DOUBLE, STRING, STRING, STRING, DOUBLE)
            //            val delimiter = guessDelimiter()
            val options = CsvReadOptions
                .builder(customerDb)
                .separator('\t')
                //                .columnTypes(types)
                .tableName("foo")
                //                .lineEnding("\n")
                .build()

            val transactions = Table.read().csv(options)


        }

        mb.asDataFrame().writeTSV(File("customers_bench_${timestamp()}_numRec$numRecords.csv"))
    }

    object OptimizeUnivox {

        @JvmStatic
        fun main(args: Array<String>) {
            val mb = MicroBenchmark<String>(reps = 1, warmupReps = 1)

            mb.elapseNano {
                val types = mapOf(
                    "customer_id" to ColType.String,
                    "product_id" to ColType.String,
                    "date_epoch" to ColType.String,
                    "transaction_amount" to ColType.String,
                    "merchant_id" to ColType.String
                )

                //                val df = DataFrame.readTsvUnivox(FileReader(customerDb(1000)), colTypes = types)
                val df = DataFrame.readTsvUnivox(FileReader(customerDb(1E6.toInt())))
                df.head().print()
                df.schema()
            }

            mb.asDataFrame().print()

        }

        object ExploreTableSaw {
            @JvmStatic
            fun main(args: Array<String>) {
                val foo: ColumnType = STRING
                //                val types = arrayOf(STRING, SKIP, STRING, STRING, STRING)
                //            val delimiter = guessDelimiterx()
                val options = CsvReadOptions
                    .builder(File("/Users/brandl/projects/kotlin/krangl/customers.txt"))
                    .separator('\t')
                    //                    .columnTypes(types)
                    .tableName("foo")
                    //                .lineEnding("\n")
                    .build()

                val transactions = Table.read().csv(options)
            }
        }
    }

    object AnalyzeBenchData {
        @JvmStatic
        fun main(args: Array<String>) {
            """
kshell_from_kscript.sh <(echo '
//DEPS com.github.holgerbrandl:kravis:0.4-SNAPSHOT
//DEPS de.mpicbg.scicomp:krangl:0.11-SNAPSHOT
//DEPS tech.tablesaw:tablesaw-core:0.25.2
')

import krangl.*
import kravis.*
            """.trimIndent()
            var benchData: DataFrame = DataFrame.readTSV("/Users/brandl/projects/kotlin/krangl/customers_bench_20180912105055.csv")
            benchData
            benchData.ggplot(x = "config", y = "mean").geomBar(stat = BarStat.identity).show()
            benchData.schema()

            benchData = benchData.addColumn("split_times") {
                it["timesMS"].map<String> {
                    it.replace("[", "").replace("]", "")
                        .split(", ").map { it.trim().toDouble() }
                }
            }
            benchData.schema()

            benchData = benchData.unnest("split_times")
            benchData.schema()


            benchData.ggplot(x = "config", y = "mean")
                .geomBar(stat = BarStat.identity)
                .geomPoint(Aes(y = "split_times"), position = PositionJitter(), alpha = .4)
                .show()


            //            DataFrame.readTSV(File("customers_bench_${timestamp()}.csv")).ggplot(x="")
        }
    }

    fun timestamp() = SimpleDateFormat("yyyyMMddHHmmss").format(Date())


    data class Customer(val customer_id: String, val product_id: Int, val data: String, val amount: Double, val merchantId: String) {

        companion object {
            fun generate(): Customer = Customer(randString(), Math.random().roundToInt(), randString(), Math.random(), randString())
        }

    }

    //    fun randString() = UUID.randomUUID().toString().substring(0, 6)
    //    fun randString() =Math.random().toString()
    fun randString() = "foo"


}


private fun customerDb(numRecords: Int = 1E7.toInt()): File {
    val outputFile = File("./customers_${numRecords}.txt")
    if (outputFile.exists()) return outputFile


    val df = Array<BenchmarkCustomers.Customer>(numRecords) { BenchmarkCustomers.Customer.generate() }.toList().asDataFrame()
    //        val df = (1..1E8.toInt()).map { Customer.generate() }.asDataFrame()

    //        outputFile.printWriter().use { out ->
    //            repeat(1E6.toInt()) { Customer.generate() }
    //        }

    //    df.filter{it["product_id"].isNA()}.print()

    println("serializing customers")
    df.writeTSV(outputFile)
    return outputFile
}

fun <T> MicroBenchmark<T>.asDataFrame(): DataFrame {
    //    return results.deparseRecords (
    //        "config" with { config },
    //        "num_reps" with { reps },
    //        "" with { times }
    //    )

    return results.asDataFrame()
}


