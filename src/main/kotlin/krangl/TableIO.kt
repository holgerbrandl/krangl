//@file:Suppress("unused")

package krangl

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.apache.commons.csv.CSVRecord
import java.io.*
import java.net.URI
import java.net.URL
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import kotlin.math.roundToInt

/**
Methods to read and write tables into/from DataFrames
 * see also https://commons.apache.org/proper/commons-csv/ for other implementations
 * https://github.com/databricks/spark-csv
 * https://examples.javacodegeeks.com/core-java/apache/commons/csv-commons/writeread-csv-files-with-apache-commons-csv-example/

 */


enum class ColType {
    Int, Long, Double, Boolean, String, Guess
}

private fun asStream(fileOrUrl: String) = (if (isURL(fileOrUrl)) {
    URL(fileOrUrl).toURI()
} else {
    File(fileOrUrl).toURI()
}).toURL().openStream()

internal fun isURL(fileOrUrl: String): Boolean = listOf("http:", "https:", "ftp:").any { fileOrUrl.startsWith(it) }


@JvmOverloads
fun DataFrame.Companion.readCSV(
    fileOrUrl: String,
    format: CSVFormat = CSVFormat.DEFAULT.withHeader(),
    colTypes: ColumnTypeSpec = GuessSpec()
) = readDelim(
    asStream(fileOrUrl),
    format = format,
    colTypes = colTypes,
    isCompressed = listOf("gz", "zip").contains(fileOrUrl.split(".").last())
)


@JvmOverloads
fun DataFrame.Companion.readTSV(
    fileOrUrl: String,
    format: CSVFormat = CSVFormat.TDF.withHeader(),
    colTypes: ColumnTypeSpec = GuessSpec()
) = readDelim(
    inStream = asStream(fileOrUrl),
    format = format,
    colTypes = colTypes,
    isCompressed = listOf("gz", "zip").contains(fileOrUrl.split(".").last())
)

@JvmOverloads
fun DataFrame.Companion.readTSV(
    file: File,
    format: CSVFormat = CSVFormat.TDF.withHeader(),
    colTypes: ColumnTypeSpec = GuessSpec()
) = readDelim(
    FileInputStream(file),
    format = format,
    colTypes = colTypes,
    isCompressed = guessCompressed(file)
)


@JvmOverloads
fun DataFrame.Companion.readCSV(
    file: File,
    format: CSVFormat = CSVFormat.DEFAULT.withHeader(),
    colTypes: ColumnTypeSpec = GuessSpec()
) = readDelim(
    inStream = FileInputStream(file),
    format = format,
    colTypes = colTypes,
    isCompressed = listOf("gz", "zip").contains(file.extension)
)


private fun guessCompressed(file: File) = listOf("gz", "zip").contains(file.extension)


// http://stackoverflow.com/questions/9648811/specific-difference-between-bufferedreader-and-filereader
fun DataFrame.Companion.readDelim(
    uri: URI,
    //                                hasHeader:Boolean =true,
    format: CSVFormat = CSVFormat.DEFAULT.withHeader(),
    isCompressed: Boolean = uri.toURL().toString().endsWith(".gz"),
    colTypes: ColumnTypeSpec = GuessSpec()
): DataFrame {

    val inputStream = uri.toURL().openStream()
    val streamReader = if (isCompressed) {
        // http://stackoverflow.com/questions/1080381/gzipinputstream-reading-line-by-line
        val gzip = GZIPInputStream(inputStream)
        InputStreamReader(gzip)
    } else {
        InputStreamReader(inputStream)
    }

    return readDelim(
        BufferedReader(streamReader),
        format = format,
        colTypes = colTypes
    )
}

//http://stackoverflow.com/questions/5200187/convert-inputstream-to-bufferedreader
fun DataFrame.Companion.readDelim(
    inStream: InputStream,
    format: CSVFormat = CSVFormat.DEFAULT.withHeader(),
    isCompressed: Boolean = false,
    colTypes: ColumnTypeSpec = GuessSpec()
) =
    if (isCompressed) {
        InputStreamReader(GZIPInputStream(inStream))
    } else {
        BufferedReader(InputStreamReader(inStream, "UTF-8"))
    }.run {
        readDelim(this, format, colTypes = colTypes)
    }


fun DataFrame.Companion.readDelim(
    reader: Reader,
    format: CSVFormat = CSVFormat.DEFAULT.withHeader(),
    colTypes: ColumnTypeSpec = GuessSpec(),
    skip: Int = 0
): DataFrame {

    val formatWithNullString = if (format.isNullStringSet) {
        format
    } else {
        format.withNullString(MISSING_VALUE)
    }

    var reader = reader
    if (skip > 0) {
        reader = BufferedReader(reader)
        repeat(skip) { reader.readLine() }
    }

    val csvParser = formatWithNullString.parse(reader)
    val records = csvParser.records

    val columnNames = csvParser.headerMap?.keys
        ?: (1..records[0].count()).map { index -> "X${index}" }

    // Make column names unique when reading them + unit test
    val uniqueNames = columnNames
        .withIndex()
        .groupBy { it.value }
        .flatMap { (grpName, columns) ->
            columns
                .mapIndexed { index, indexedValue ->
                    indexedValue.index to (grpName + if (index > 2) "_${index + 2}" else "")
                }
        }
        .sortedBy { it.first }.map { it.second }


    //    csvParser.headerMap.keys.pmap{colName ->
    val cols = uniqueNames.mapIndexed { colIndex, colName ->
        val colType = colTypes.typeOf(colIndex, colName)

        dataColFactory(colName, colIndex, colType, records)
    }

    return SimpleDataFrame(cols)
}

/** Allows specifying the column types when reading a data-frame. */
sealed class ColumnTypeSpec {
    abstract fun typeOf(index: Int, columnName: String): ColType
}

class GuessSpec : NamedColumnSpec(mapOf())

open class NamedColumnSpec(val colTypes: Map<String, ColType>) : ColumnTypeSpec() {
    val DEFAULT_COLUMN_SPEC_NAME = ".default"

    val DEFAULT_TYPE = colTypes[DEFAULT_COLUMN_SPEC_NAME] ?: ColType.Guess

    override fun typeOf(index: Int, columnName: String): ColType {

        return colTypes[columnName] ?: DEFAULT_TYPE
    }

    constructor(vararg spec: Pair<String, ColType>) : this(spec.toMap())
}

/**
 * CompactColumnSpec uses a compact string representation where each character represents one column:
 *
 * c = character
 *
 * i = integer
 *
 * n = number
 *
 * d = double
 *
 * l = logical
 *
 * f = factor
 *
 * D = date
 *
 * T = date time
 *
 * t = time
 *
 * ? = guess
 *
 * _ or - = skip
 */
class CompactColumnSpec(val columnSpec: String) : ColumnTypeSpec() {

    val colTypes = columnSpec.map {
        when (it) {
            's' -> ColType.String
            'i' -> ColType.Int
            'l' -> ColType.Long
            'd' -> ColType.Double
            'b' -> ColType.Boolean
            // TODO natively support reading date columns
//            'D' -> date
//            'T' -> date time
//            't' -> time
            '?' -> ColType.Guess
//            '_' -> - = skip
            else -> throw IllegalArgumentException("invalid type '$it' in compact column spec '$columnSpec'")
        }
    }

    override fun typeOf(index: Int, columnName: String): ColType {
        return colTypes[index]
    }
}


fun DataFrame.Companion.readFixedWidth(
    reader: Reader,
    format: List<Pair<String?, Int>>,
    colTypes: Map<String, ColType> = mapOf(),
    missingValue: String = MISSING_VALUE,
    skip: Int = 0
): DataFrame {
    var reader = reader
    if (skip > 0) {
        reader = BufferedReader(reader)
        repeat(skip) { reader.readLine() }
    }

    val splitPositions = (listOf(0) + format.map { it.second }).cumSum().map { it.roundToInt() }.zipWithNext()

    val padLength = format.map { it.second }.sum()

    val records: List<List<String?>> = reader.readLines().map {
        it.padEnd(padLength)
    }.map { line ->
        splitPositions.map {
            val cell = line.substring(it.first, it.second).trim()
            if (cell == missingValue) null else cell
        }
    }
    reader.close()

    // fill missing header
    val columnNames = format.map { it.first }.mapIndexed { index, name -> name ?: "X${index}" }

    // Make column names unique when reading them + unit test
    val uniqueNames = columnNames
        .withIndex()
        .groupBy { it.value }
        .flatMap { (grpName, columns) ->
            columns
                .mapIndexed { index, indexedValue ->
                    indexedValue.index to (grpName + if (index > 2) "_${index + 2}" else "")
                }
        }
        .sortedBy { it.first }.map { it.second }


    val cols = uniqueNames.mapIndexed { colIndex, colName ->
        val defaultColType = colTypes[".default"] ?: ColType.Guess

        val colType = colTypes[colName] ?: defaultColType

        // extract the column
        val columnData: Array<String?> = records.map { it[colIndex] }.toList().toTypedArray()

        dataColFactory(colName, colType, columnData)
    }

    return SimpleDataFrame(cols)
}


val MISSING_VALUE = "NA"

// NA aware conversions
internal fun String.naAsNull(): String? = if (this == MISSING_VALUE) null else this

internal fun String?.nullAsNA(): String = this ?: MISSING_VALUE

//
//inline fun <reified T>  DataFrame.replaceNA(value: T, noinline columnSelect: ColumnSelector = { all() }) =
//    colSelectAsNames(columnSelect).fold(this) { df, column, ->
//        df.addColumn(column) {
//            it[column].values().map {
//                it ?: value
//            }
//        }
//    }

internal fun String?.cellValueAsBoolean(): Boolean? {
    if (this == null) return null

    var cellValue: String? = toUpperCase()

    cellValue = if (cellValue == "F") "FALSE" else cellValue
    cellValue = if (cellValue == "T") "TRUE" else cellValue

    if (!listOf("TRUE", "FALSE", null).contains(cellValue)) throw NumberFormatException("invalid boolean cell value")

    return cellValue?.toBoolean()
}

internal fun guessColType(firstElements: List<String>): ColType =
    when {
        isBoolCol(firstElements) -> ColType.Boolean
        isIntCol(firstElements) -> ColType.Int
        isLongCol(firstElements) -> ColType.Long
        isDoubleCol(firstElements) -> ColType.Double
        else -> ColType.String
    }


internal fun dataColFactory(
    colName: String,
    colIndex: Int,
    colType: ColType,
    records: MutableList<CSVRecord>
): DataCol =
    when (colType) {
        // see https://github.com/holgerbrandl/krangl/issues/10
        ColType.Int -> try {
            IntCol(colName, records.map { it[colIndex]?.toInt() })
        } catch (e: NumberFormatException) {
            StringCol(colName, records.map { it[colIndex] })
        }
        ColType.Long -> try {
            LongCol(colName, records.map { it[colIndex]?.toLong() })
        } catch (e: NumberFormatException) {
            StringCol(colName, records.map { it[colIndex] })
        }

        ColType.Double -> DoubleCol(colName, records.map { it[colIndex]?.toDouble() })

        ColType.Boolean -> BooleanCol(colName, records.map { it[colIndex]?.cellValueAsBoolean() })

        ColType.String -> StringCol(colName, records.map { it[colIndex] })

        ColType.Guess -> dataColFactory(colName, colIndex, guessColType(peekCol(colIndex, records)), records)
    }


// TODO add missing value support with user defined string (e.g. NA here) here

internal fun dataColFactory(colName: String, colType: ColType, records: Array<*>, guessMax: Int = 100): DataCol =
    when (colType) {
        // see https://github.com/holgerbrandl/krangl/issues/10
        ColType.Int -> try {
            IntCol(colName, records.map { it.trimEmptyToNull()?.toInt() })
        } catch (e: NumberFormatException) {
            StringCol(colName, records.map { it?.toString() })
        }
        ColType.Long -> try {
            LongCol(colName, records.map { it.trimEmptyToNull()?.toLong() })
        } catch (e: NumberFormatException) {
            StringCol(colName, records.map { it?.toString() })
        }

        ColType.Double -> DoubleCol(colName, records.map { it.trimEmptyToNull()?.toDouble() })

        ColType.Boolean -> BooleanCol(colName, records.map { it?.toString()?.cellValueAsBoolean() })

        ColType.String -> StringCol(colName, records.map { it?.toString() })

        ColType.Guess -> dataColFactory(colName, guessColType(peekCol(records, guessMax)), records)
    }

private fun Any?.trimEmptyToNull(): String? = this?.toString()?.trim()?.ifBlank { null }

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

internal fun isLongCol(firstElements: List<String?>): Boolean = try {
    firstElements.map { it?.toLong() }; true
} catch (e: NumberFormatException) {
    false
}

internal fun isBoolCol(firstElements: List<String?>): Boolean = try {
    firstElements.map { it?.cellValueAsBoolean() }; true
} catch (e: NumberFormatException) {
    false
}


internal fun peekCol(colIndex: Int, records: List<CSVRecord>, peekSize: Int = 100) = records
    .asSequence()
    .mapIndexed { rowIndex, _ -> records[rowIndex][colIndex] }
    .filterNotNull()
    .take(peekSize)
    .toList()

internal fun peekCol(records: Array<*>, peekSize: Int = 100) = records
    .asSequence()
    .map { it.toString() }
    .filterNotNull()
    .take(peekSize)
    .toList()

fun DataFrame.writeTSV(
    file: File,
    format: CSVFormat = CSVFormat.TDF.withHeader(*names.toTypedArray())
) = writeCSV(file, format)


fun DataFrame.writeCSV(
    file: File,
    format: CSVFormat = CSVFormat.DEFAULT.withHeader(*names.toTypedArray())
) {
    @Suppress("NAME_SHADOWING")
    val format = if (format.run { header != null && header.size == 0 }) {
        warning("[krangl] Adding missing column names to csv format")
        format.withHeader(*names.toTypedArray())
    } else {
        format
    }

    val compress: Boolean = listOf("gz", "zip").contains(file.extension)

    val p =
        if (!compress) PrintWriter(file) else BufferedWriter(OutputStreamWriter(GZIPOutputStream(FileOutputStream(file))))

    //initialize CSVPrinter object
    val csvFilePrinter = CSVPrinter(p, format)

    // write records
    for (record in rowData()) {
        csvFilePrinter.printRecord(record)
    }

    p.flush()
    p.close()
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
- sleep_total. total amount of sleep, in hours
- sleep_rem. rem sleep, in hours
- sleep_cycle. length of sleep cycle, in hours
- awake. amount of time spent awake, in hours
- brainwt. brain weight in kilograms
- bodywt. body weight in kilograms
 */
val sleepData by lazy { DataFrame.readDelim(DataFrame::class.java.getResourceAsStream("data/msleep.csv")) }


/* Data class required to parse sleep Data records. */
data class SleepPattern(
    val name: String,
    val genus: String,
    val vore: String?,
    val order: String,
    val conservation: String?,
    val sleep_total: Double,
    val sleep_rem: Double?,
    val sleep_cycle: Double?,
    val awake: Double,
    val brainwt: Double?,
    val bodywt: Double
)

val sleepPatterns by lazy {
    sleepData.rows.map { row ->
        SleepPattern(
            row["name"] as String,
            row["genus"] as String,
            row["vore"] as String?,
            row["order"] as String,
            row["conservation"] as String?,
            row["sleep_total"] as Double,
            row["sleep_rem"] as Double?,
            row["sleep_cycle"] as Double?,
            row["awake"] as Double,
            row["brainwt"] as Double?,
            row["bodywt"] as Double
        )
    }
}


/**
 * This famous (Fisher's or Anderson's) iris data set gives the measurements in centimeters
 * of the variables sepal length and width and petal length and width, respectively, for 50
 * flowers from each of 3 species of iris. The species are Iris setosa, versicolor, and virginica.
 *
 * ## Format
 *
 * iris is a data frame with 150 cases (rows) and 5 variables (columns) named `Sepal.Length`, `Sepal.Width`, `Petal.Length`, `Petal.Width`, and `Species`.
 *
 * ## Source
 * Fisher, R. A. (1936) The use of multiple measurements in taxonomic problems. Annals of Eugenics, 7, Part II, 179–188.
 *
 * The data were collected by Anderson, Edgar (1935). The irises of the Gaspe Peninsula, Bulletin of the American Iris Society, 59, 2–5.
 *
 * ## References
 * Becker, R. A., Chambers, J. M. and Wilks, A. R. (1988) The New S Language. Wadsworth & Brooks/Cole. (has iris3 as iris.)
 *
 *
 */
val irisData by lazy {
    DataFrame.readDelim(
        DataFrame::class.java.getResourceAsStream("data/iris.txt"),
        format = CSVFormat.TDF.withHeader()
    )
}


/**
On-time data for all 336776 flights that departed NYC (i.e. JFK, LGA or EWR) in 2013.

Adopted from r, see `nycflights13::flights`
 */


internal val cacheDataDir by lazy {
    File(System.getProperty("user.home"), ".krangl_example_data").apply { if (!isDirectory()) mkdir() }
}

internal val flightsCacheFile = File(cacheDataDir, ".flights_data.tsv.gz")

/**
 * On-time data for all flights that departed NYC (i.e. JFK, LGA or EWR) in 2013.
 *
 * * `year`, `month`,day: Date of departure
 * * `dep_time`, `arr_time`: Actual departure and arrival times, local tz.
 * * `sched_dep_time`, `sched_arr_time`: Scheduled departure and arrival times, local tz.
 * * `dep_delay`, `arr_delay`: Departure and arrival delays, in minutes. Negative times represent early departures/arrivals.
 * * `hour`, `minute`: Time of scheduled departure broken into hour and minutes.
 * * `carrier`: Two letter carrier abbreviation. See airlines to get name
 * * `tailnum`: Plane tail number
 * * `flight`: Flight number
 * * `origin`,dest: Origin and destination. See airports for additional metadata.
 * * `air_time`: Amount of time spent in the air, in minutes
 * * `distance`: Distance between airports, in miles
 * * `time_hour`: Scheduled date and hour of the flight as a POSIXct date. Along with origin, can be used to join flights data to weather data.
 *
 *
 * ### Source
 *
 * * RITA, Bureau of transportation statistics, http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236
 * * https://github.com/hadley/nycflights13
 */
val flightsData by lazy {

    if (!flightsCacheFile.isFile) {
        warning("[krangl] Downloading flights data into local cache...", false)
        val flightsURL =
            URL("https://github.com/holgerbrandl/krangl/blob/v0.4/src/test/resources/krangl/data/nycflights.tsv.gz?raw=true")
        warning("Done!")


        //    for progress monitoring use
        //    https@ //stackoverflow.com/questions/12800588/how-to-calculate-a-file-size-from-url-in-java

        flightsCacheFile.writeBytes(flightsURL.readBytes())
    }


    DataFrame.readTSV(flightsCacheFile)

    // consider to use progress bar here
}

// todo support Read and write data using Tablesaw’s “.saw” format --> use dedicated artifact to minimize dependcies