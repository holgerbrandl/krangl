//@file:Suppress("unused")

package krangl

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.apache.commons.csv.CSVRecord
import java.io.*
import java.net.URL
import java.util.zip.GZIPInputStream

/**
Methods to read and write tables into/from DataFrames
 * see also https://commons.apache.org/proper/commons-csv/ for other implementations
 * https://github.com/databricks/spark-csv
 * https://examples.javacodegeeks.com/core-java/apache/commons/csv-commons/writeread-csv-files-with-apache-commons-csv-example/

 */


fun DataFrame.Companion.fromCSV(file: String) = fromCSV(File(file))
fun DataFrame.Companion.fromTSV(file: String) = fromTSV(File(file))

fun DataFrame.Companion.fromTSV(file: File) = fromCSV(file, format = CSVFormat.TDF.withHeader())

// http://stackoverflow.com/questions/9648811/specific-difference-between-bufferedreader-and-filereader
fun DataFrame.Companion.fromCSV(file: File,
                                format: CSVFormat = CSVFormat.DEFAULT.withHeader(),
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
fun DataFrame.Companion.fromCSV(inStream: InputStream, format: CSVFormat = CSVFormat.DEFAULT.withHeader(), isCompressed: Boolean = false) =
        if (isCompressed) {
            InputStreamReader(GZIPInputStream(inStream))
        } else {
            BufferedReader(InputStreamReader(inStream, "UTF-8"))
        }.run {
            fromCSV(this, format)
        }


fun DataFrame.Companion.fromCSV(reader: Reader, format: CSVFormat = CSVFormat.DEFAULT.withHeader()): DataFrame {
    val csvParser = format.parse(reader)

    val records = csvParser.records

    val cols = mutableListOf<DataCol>()

    // todo also support reading files without header --> use generic column names if so

    val columnNames = csvParser.headerMap?.keys ?:
            (1..csvParser.records[0].count()).mapIndexed { index, _ -> "X${index}" }


    // todo make column names unique when reading them + unit test

    //    csvParser.headerMap.keys.pmap{colName ->
    for (colName in columnNames) {
        val firstElements = peekCol(colName, records)

        when {
        // see https://github.com/holgerbrandl/krangl/issues/10
            isIntCol(firstElements) -> try {
                IntCol(colName, records.map { it[colName].naAsNull()?.toInt() })
            } catch (e: NumberFormatException) {
                StringCol(colName, records.map { it[colName].naAsNull() })
            }
            isDoubleCol(firstElements) -> DoubleCol(colName, records.map { it[colName].naAsNull()?.toDouble() })
            isBoolCol(firstElements) -> BooleanCol(colName, records.map { it[colName].naAsNull()?.cellValueAsBoolean() })
            else -> StringCol(colName, records.map { it[colName].naAsNull() })
        }.let { cols.add(it) }

    }

    return SimpleDataFrame(cols)
}

/** Create a data-frame from a list of objects */
fun <T> List<T>.asDataFrame(mapping: (T) -> DataFrameRow) = DataFrame.of(this, mapping)

/** Create a data-frame from a list of objects */
fun <T> DataFrame.Companion.of(records: List<T>, mapping: (T) -> DataFrameRow): DataFrame {
    val rowData = records.map { mapping(it) }
    val columnNames = mapping(records.first()).keys

    val columnData = columnNames.map { it to emptyList<Any?>().toMutableList() }.toMap()

    for (record in rowData) {
        columnData.forEach { colName, colData -> colData.add(record[colName]) }
    }

    return columnData.map { (name, data) -> handleListErasure(name, data) }.asDataFrame()
}


val MISSING_VALUE = "NA"

// NA aware conversions
internal fun String.naAsNull(): String? = if (this == MISSING_VALUE) null else this

internal fun String?.nullAsNA(): String = this ?: MISSING_VALUE

internal fun String?.cellValueAsBoolean(): Boolean? {
    if (this == null) return null

    var cellValue: String? = toUpperCase()

    cellValue = if (cellValue == "NA") null else cellValue
    cellValue = if (cellValue == "F") "false" else cellValue
    cellValue = if (cellValue == "T") "true" else cellValue

    if (!listOf("true", "false", null).contains(cellValue)) throw NumberFormatException("invalid boolean cell value")

    return cellValue?.toBoolean()
}


// TODO add missing value support with user defined string (e.g. NA here) here

internal fun isDoubleCol(firstElements: List<String?>): Boolean = try {
    firstElements.map { it?.toDouble() }; true
} catch (e: NumberFormatException) {
    false
}

internal fun isIntCol(firstElements: List<String?>): Boolean = try {
    firstElements.map { it?.toInt() }; true
} catch (e: NumberFormatException) {
    false
}

internal fun isBoolCol(firstElements: List<String?>): Boolean = try {
    firstElements.map { it?.cellValueAsBoolean() }; true
} catch (e: NumberFormatException) {
    false
}


// todo keep peeking until we hit the first/N non NA value
internal fun peekCol(colName: String?, records: List<CSVRecord>, peekSize: Int = 5) = records.take(peekSize).mapIndexed { rowIndex, csvRecord -> records[rowIndex][colName].naAsNull() }


//TODO add support for compressed writing

fun DataFrame.writeCSV(file: String, format: CSVFormat = CSVFormat.DEFAULT, colNames: Boolean = true) = writeCSV(File(file), format, colNames)

fun DataFrame.writeCSV(file: File, format: CSVFormat = CSVFormat.DEFAULT, colNames: Boolean = true) {
    //initialize FileWriter object
    val fileWriter = FileWriter(file)

    //initialize CSVPrinter object
    val csvFilePrinter = CSVPrinter(fileWriter, format)

    //Create CSV file header
    if (colNames) csvFilePrinter.printRecord(names)

    // write records
    for (record in rowData()) {
        csvFilePrinter.printRecord(record)
    }

    fileWriter.flush()
    fileWriter.close()
    csvFilePrinter.close()
}


/**
An example data frame with 83 rows and 11 variables

This is an updated and expanded version of the mammals sleep dataset. Updated sleep times and weights were taken from V. M. Savage and G. B. West. A quantitative, theoretical framework for understanding mammalian sleep. Proceedings of the National Academy of Sciences, 104 (3):1051-1056, 2007.

Additional variables order, conservation status and vore were added from wikipedia.
- name. common name
- genus.
- vore. carnivore, omnivore or herbivore?
- order.
- conservation. the conservation status of the animal
- sleep\_total. total amount of sleep, in hours
- sleep\_rem. rem sleep, in hours
- sleep\_cycle. length of sleep cycle, in hours
- awake. amount of time spent awake, in hours
- brainwt. brain weight in kilograms
- bodywt. body weight in kilograms
 */
val sleepData by lazy { DataFrame.fromCSV(DataFrame::class.java.getResourceAsStream("data/msleep.csv"), CSVFormat.DEFAULT.withHeader()) }

/**
On-time data for all 336776 flights that departed NYC (i.e. JFK, LGA or EWR) in 2013.

Adopted from r, see `nycflights13::flights`
 */
internal val flightsCacheFile = File(System.getProperty("user.home"), ".flights_data.tsv.gz")

val flightsData by lazy{

    if(!flightsCacheFile.isFile) {
        warning("[krangl] Downloading flights data into local cache...", false)
        val flightsURL = URL("https://github.com/holgerbrandl/krangl/blob/v0.4/src/test/resources/krangl/data/nycflights.tsv.gz?raw=true")
        warning("Done!")


        //    for progress monitoring use
        //    https@ //stackoverflow.com/questions/12800588/how-to-calculate-a-file-size-from-url-in-java
        flightsCacheFile.writeBytes(flightsURL.readBytes())
    }


    DataFrame.fromTSV(flightsCacheFile)

    // consider to use progress bar here
}

// todo support Read and write data using Tablesaw’s “.saw” format