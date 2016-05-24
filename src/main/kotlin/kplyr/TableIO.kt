package kplyr

import format
import mean
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.apache.commons.csv.CSVRecord
import sd
import java.io.*
import java.util.zip.GZIPInputStream

/**
Methods to read and write tables into/from DataFrames
 * see also https://commons.apache.org/proper/commons-csv/ for other implementations
 * https://github.com/databricks/spark-csv
 * https://examples.javacodegeeks.com/core-java/apache/commons/csv-commons/writeread-csv-files-with-apache-commons-csv-example/

 */


fun DataFrame.Companion.fromCSV(file: String) = fromCSV(File(file))

fun DataFrame.Companion.fromTSV(file: String) = fromCSV(File(file), format = CSVFormat.TDF)

// http://stackoverflow.com/questions/9648811/specific-difference-between-bufferedreader-and-filereader
fun DataFrame.Companion.fromCSV(file: File,
                                format: CSVFormat = CSVFormat.DEFAULT,
                                isCompressed: Boolean = file.name.endsWith(".gz")): DataFrame {

    val bufReader = if (isCompressed) {
        // http://stackoverflow.com/questions/1080381/gzipinputstream-reading-line-by-line
        val gzip = GZIPInputStream(FileInputStream(file));
        BufferedReader(InputStreamReader(gzip));
    } else {
        BufferedReader(FileReader(file))
    }

    return fromCSV(bufReader, format)
}

//http://stackoverflow.com/questions/5200187/convert-inputstream-to-bufferedreader
fun DataFrame.Companion.fromCSV(inStream: InputStream, format: CSVFormat = CSVFormat.DEFAULT) =
        fromCSV(BufferedReader(InputStreamReader(inStream, "UTF-8")), format)


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

internal fun DataFrame.Companion.fromCSV(reader: Reader, format: CSVFormat = CSVFormat.DEFAULT): DataFrame {
    val csvParser = format.withFirstRecordAsHeader().parse(reader)

    val records = csvParser.records

//     guess column types and trigger data conversion
    val numRows = records.size
    val cols = mutableListOf<DataCol>()

    for (colName in csvParser.headerMap.keys) {
        val firstElements = peekCol(colName, records)
        when { // when without arg see https://kotlinlang.org/docs/reference/control-flow.html#when-expression
            isIntCol(firstElements) -> IntCol(colName, Array(numRows, { records[it][colName].naAsNull()?.toInt() }).toList())
            isDoubleCol(firstElements) -> DoubleCol(colName, Array(numRows, { records[it][colName].naAsNull()?.toDouble() }).toList())
            isBoolCol(firstElements) -> BooleanCol(colName, Array(numRows, { records[it][colName].naAsNull().cellValueAsBoolean() }).toList())
            else -> StringCol(colName, Array(numRows, { records[it][colName].naAsNull() }).toList())
        }.let { cols.add(it) }
    }

    return SimpleDataFrame(cols)
}


// NA aware conversions
internal fun String.naAsNull(): String? = if (this == "NA") null else this

internal fun String?.cellValueAsBoolean(): Boolean? {
    if (this == null) return null

    var cellValue: String? = toUpperCase()

    cellValue = if (cellValue == "NA") null else cellValue
    cellValue = if (cellValue == "F") "false" else cellValue
    cellValue = if (cellValue == "T") "true" else cellValue

    if (!listOf("true", "false", null).contains(cellValue)) throw NumberFormatException("invalid boolean cell value")

    return if (cellValue == "NA") null else cellValue!!.toBoolean()
}




// TODO add missing value support with user defined string (e.g. NA here) here

internal fun isDoubleCol(firstElements: List<String?>): Boolean = try {
    firstElements.map { it?.toDouble() };  true
} catch(e: NumberFormatException) {
    false
}

internal fun isIntCol(firstElements: List<String?>): Boolean = try {
    firstElements.map { it?.toInt() };  true
} catch(e: NumberFormatException) {
    false
}

internal fun isBoolCol(firstElements: List<String?>): Boolean = try {
    firstElements.map { it?.cellValueAsBoolean() };  true
} catch(e: NumberFormatException) {
    false
}

internal fun peekCol(colName: String?, records: List<CSVRecord>, peekSize: Int = 5) = records.take(peekSize).mapIndexed { rowIndex, csvRecord -> records[rowIndex][colName] }


//TODO add support for compressed writing

fun DataFrame.writeCSV(file: String, format: CSVFormat = CSVFormat.DEFAULT) = writeCSV(File(file), format)

fun DataFrame.writeCSV(file: File, format: CSVFormat = CSVFormat.DEFAULT) {
    //initialize FileWriter object
    val fileWriter = FileWriter(file)

    //initialize CSVPrinter object
    val csvFilePrinter = CSVPrinter(fileWriter, format)

    //Create CSV file header
    csvFilePrinter.printRecord(names)

    // write records
    for (record in rows) {
        csvFilePrinter.printRecord(record.values)
    }

    fileWriter.flush()
    fileWriter.close()
    csvFilePrinter.close()
}


data class RunTimes<T>(val result: T, val runtimes: List<Float>) {
    val mean by lazy { runtimes.mean() }

    override fun toString(): String {
        // todo use actual confidence interval here
        return "${mean.format(2)} ± ${runtimes.sd()?.format(2)} SD\t "
    }

    companion object {
        inline fun <R> measure(block: () -> R, numRuns: Int = 1): RunTimes<R> {
            require(numRuns > 0)

            var result: R? = null

            val runs = (1..numRuns).map {
                val start = System.currentTimeMillis()
                result = block()
                (System.currentTimeMillis() - start) / 1000.toFloat()
            }

            return RunTimes<R>(result!!, runs)
        }

    }
}

fun main(args: Array<String>) {

    fun getFlightsReader(): BufferedReader {
        val flightsFile = File("/Users/brandl/projects/kotlin/kplyr/src/test/resources/kplyr/data/nycflights.tsv.gz")
        val gzip = GZIPInputStream(FileInputStream(flightsFile));
        return BufferedReader(InputStreamReader(gzip));
    }

    // jvm warmup
//    DataFrame.fromCSV(getFlightsReader(), CSVFormat.TDF).head(5).writeCSV("flights_head.txt", format = CSVFormat.TDF)
    repeat(100) { DataFrame.fromTSV("flights_head.txt") }

    repeat(3) {
        println("another run:")

        RunTimes.measure({
            DataFrame.fromCSVlistArray(getFlightsReader(), CSVFormat.TDF)
        }, 8).apply {
            println("fromCSVlistArray: $this")
        }.result//.glimpse()

        RunTimes.measure({
            DataFrame.fromCSVArray(getFlightsReader(), CSVFormat.TDF)
        }, 8).apply {
            println("fromCSVArray: $this")
        }.result//.glimpse()

        RunTimes.measure({
            DataFrame.fromCSV(getFlightsReader(), CSVFormat.TDF)
        }, 8).apply {
            println("fromCSV: $this")
        }.result//.glimpse()
    }

    val df = SimpleDataFrame()
//    df.appendRow(mapOf("config" to 1))
}


//tdodo finish row append ehtod
//fun SimpleDataFrame.appendRow(mapOf: Map<String, Int>) {
//    require(names.e)
//
//
//}


//internal fun <T, R> Iterable<T>.pmap(
//        numThreads: Int = Runtime.getRuntime().availableProcessors() - 2,
//        exec: ExecutorService = Executors.newFixedThreadPool(numThreads),
//        transform: (T) -> R): List<R> {
//
//    // default size is just an inlined version of kotlin.collections.collectionSizeOrDefault
//    val defaultSize = if (this is Collection<*>) this.size else 10
//    val destination = Collections.synchronizedList(ArrayList<R>(defaultSize))
//
//    for (item in this) {
//        exec.submit { destination.add(transform(item)) }
//    }
//
//    exec.shutdown()
//    exec.awaitTermination(1, TimeUnit.DAYS)
//
//    return ArrayList<R>(destination)
//}