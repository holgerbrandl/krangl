//@file:Suppress("unused")

package krangl

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.apache.commons.csv.CSVRecord
import java.io.*
import java.net.URI
import java.net.URL
import java.util.zip.GZIPInputStream

/**
Methods to read and write tables into/from DataFrames
 * see also https://commons.apache.org/proper/commons-csv/ for other implementations
 * https://github.com/databricks/spark-csv
 * https://examples.javacodegeeks.com/core-java/apache/commons/csv-commons/writeread-csv-files-with-apache-commons-csv-example/

 */


enum class ColType {
    Int, Double, Boolean, String, Guess
}

private fun asStream(fileOrUrl: String) = (if (isURL(fileOrUrl)) {
    URL(fileOrUrl).toURI()
} else {
    File(fileOrUrl).toURI()
}).toURL().openStream()

private fun isURL(fileOrUrl: String): Boolean = listOf("http:", "https:", "ftp:").any { fileOrUrl.startsWith(it) }


fun DataFrame.Companion.fromCSV(fileOrUrl: String, colTypes: Map<String,ColType> = mapOf()) = asStream(fileOrUrl).run { fromCSV(this, colTypes = colTypes) }

fun DataFrame.Companion.fromTSV(fileOrUrl: String) = asStream(fileOrUrl).run { fromCSV(this, format = CSVFormat.TDF.withHeader()) }

fun DataFrame.Companion.fromCSV(file: File, colTypes: Map<String,ColType> = mapOf()) = fromCSV(FileInputStream(file), format = CSVFormat.DEFAULT.withHeader(), colTypes = colTypes)
fun DataFrame.Companion.fromTSV(file: File, colTypes: Map<String,ColType> = mapOf()) = fromCSV(FileInputStream(file), format = CSVFormat.TDF.withHeader(), colTypes = colTypes)

// http://stackoverflow.com/questions/9648811/specific-difference-between-bufferedreader-and-filereader
fun DataFrame.Companion.fromCSV(uri: URI,
//                                hasHeader:Boolean =true,
                                format: CSVFormat = CSVFormat.DEFAULT.withHeader(),
                                isCompressed: Boolean = uri.toURL().toString().endsWith(".gz"),
                                colTypes: Map<String, ColType> = mapOf()): DataFrame {

    val inputStream = uri.toURL().openStream()
    val streamReader = if (isCompressed) {
        // http://stackoverflow.com/questions/1080381/gzipinputstream-reading-line-by-line
        val gzip = GZIPInputStream(inputStream)
        InputStreamReader(gzip)
    } else {
        InputStreamReader(inputStream)
    }

    return fromCSV(BufferedReader(streamReader), format, colTypes = colTypes)
}

//http://stackoverflow.com/questions/5200187/convert-inputstream-to-bufferedreader
fun DataFrame.Companion.fromCSV(inStream: InputStream, format: CSVFormat = CSVFormat.DEFAULT.withHeader(), isCompressed: Boolean = false, colTypes: Map<String, ColType> = mapOf()) =
        if (isCompressed) {
            InputStreamReader(GZIPInputStream(inStream))
        } else {
            BufferedReader(InputStreamReader(inStream, "UTF-8"))
        }.run {
            fromCSV(this, format, colTypes = colTypes)
        }


fun DataFrame.Companion.fromCSV(reader: Reader, format: CSVFormat = CSVFormat.DEFAULT.withHeader(), colTypes: Map<String, ColType> = mapOf()): DataFrame {
    val csvParser = format.parse(reader)

    val records = csvParser.records

    val cols = mutableListOf<DataCol>()

    // todo also support reading files without header --> use generic column names if so

    val columnNames = csvParser.headerMap?.keys ?:
    (1..csvParser.records[0].count()).mapIndexed { index, _ -> "X${index}" }


    // todo make column names unique when reading them + unit test

    //    csvParser.headerMap.keys.pmap{colName ->
    for (colName in columnNames) {
        val defaultColType = colTypes[".default"] ?: ColType.Guess

        val colType = colTypes[colName] ?: defaultColType

        val column = dataColFactory(colName, colType, records)

        cols.add(column)
    }
    return SimpleDataFrame(cols)
}


val MISSING_VALUE = "NA"

// NA aware conversions
internal fun String.naAsNull(): String? = if (this == MISSING_VALUE) null else this

internal fun String?.nullAsNA(): String = this ?: MISSING_VALUE

internal fun String?.cellValueAsBoolean(): Boolean? {
    if (this == null) return null

    var cellValue: String? = toUpperCase()

    cellValue = if (cellValue == "NA") null else cellValue
    cellValue = if (cellValue == "F") "FALSE" else cellValue
    cellValue = if (cellValue == "T") "TRUE" else cellValue

    if (!listOf("TRUE", "FALSE", null).contains(cellValue)) throw NumberFormatException("invalid boolean cell value")

    return cellValue?.toBoolean()
}

internal fun guessColType(firstElements: List<String?>): ColType =
        when {
            isBoolCol(firstElements) -> ColType.Boolean
            isIntCol(firstElements) -> ColType.Int
            isDoubleCol(firstElements) -> ColType.Double
            else -> ColType.String
        }


internal fun dataColFactory(colName: String, colType: ColType, records: MutableList<CSVRecord>): DataCol =
        when (colType) {
        // see https://github.com/holgerbrandl/krangl/issues/10
            ColType.Int -> try {
                IntCol(colName, records.map { it[colName].naAsNull()?.toInt() })
            } catch (e: NumberFormatException) {
                StringCol(colName, records.map { it[colName].naAsNull() })
            }

            ColType.Double -> DoubleCol(colName, records.map { it[colName].naAsNull()?.toDouble() })

            ColType.Boolean -> BooleanCol(colName, records.map { it[colName].naAsNull()?.cellValueAsBoolean() })

            ColType.String -> StringCol(colName, records.map { it[colName].naAsNull() })

            ColType.Guess -> dataColFactory(colName, guessColType(peekCol(colName,records)), records)

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
internal fun peekCol(colName: String?, records: List<CSVRecord>, peekSize: Int = 5) = records.take(peekSize).mapIndexed { rowIndex, _ -> records[rowIndex][colName].naAsNull() }


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

val irisData by lazy { DataFrame.fromCSV(DataFrame::class.java.getResourceAsStream("data/iris.txt"), format = CSVFormat.TDF.withHeader())}


/**
On-time data for all 336776 flights that departed NYC (i.e. JFK, LGA or EWR) in 2013.

Adopted from r, see `nycflights13::flights`
 */
internal val flightsCacheFile = File(System.getProperty("user.home"), ".flights_data.tsv.gz")

val flightsData by lazy {

    if (!flightsCacheFile.isFile) {
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