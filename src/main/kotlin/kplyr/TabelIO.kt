package kplyr

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVRecord
import java.io.File
import java.io.FileReader

/**
 * Methods to read and write tables into/from DataFrames
 */


// see also https://commons.apache.org/proper/commons-csv/ for other implementations
// https://github.com/databricks/spark-csv
// todo mabye these factory method should be part of DataFrame namespace
fun fromCSV(file: String) = fromCSV(File(file))

fun fromTSV(file: String) = fromCSV(File(file), csvFormat = CSVFormat.TDF)

fun fromCSV(file: File, csvFormat: CSVFormat = CSVFormat.RFC4180): DataFrame {

    val fileReader = FileReader(file)
    val csvParser = csvFormat.withFirstRecordAsHeader().parse(fileReader)
    val records = csvParser.iterator().asSequence().toList()


    val numRows = records.toList().size
    val cols = mutableListOf<DataCol>()
    // fixme: super-inefficient because of incorrect loop order
    for (colName in csvParser.headerMap.keys) {
        when { // when without arg see https://kotlinlang.org/docs/reference/control-flow.html#when-expression
            isDoubleCol(colName, records) -> DoubleCol(colName, Array(numRows, { records[it][colName].toDoubleOrNull() }).toList())
            isIntCol(colName, records) -> IntCol(colName, Array(numRows, { records[it][colName].toIntOrNull() }).toList())
            isBoolCol(colName, records) -> BooleanCol(colName, Array(numRows, { records[it][colName].toAsBoolNull() }).toList())
            else -> StringCol(colName, Array(numRows, { records[it][colName].naAsNull() }).toList())
        }.let { cols.add(it) }
    }

    return SimpleDataFrame(cols)
}

// we use null to encode NA // todo consider NaN
// NA aware conversions
fun String.toDoubleOrNull(): Double? = if (this == "NA") null else this.toDouble()

fun String.toIntOrNull(): Int? = if (this == "NA") null else this.toInt()
fun String.toAsBoolNull(): Boolean? = if (this == "NA") null else this.toBoolean()
fun String.naAsNull(): String? = if (this == "NA") null else this

internal fun isDoubleCol(colName: String?, records: List<CSVRecord>, peekSize: Int = 5): Boolean {
    try {
        records.take(peekSize).mapIndexed { rowIndex, csvRecord -> records[rowIndex][colName].toDoubleOrNull() }
    } catch(e: NumberFormatException) {
        return false
    }

    return true
}

internal fun isIntCol(colName: String?, records: List<CSVRecord>, peekSize: Int = 5): Boolean {
    try {
        records.take(peekSize).mapIndexed { rowIndex, csvRecord -> records[rowIndex][colName].toIntOrNull() }
    } catch(e: NumberFormatException) {
        return false
    }

    return true
}

internal fun isBoolCol(colName: String?, records: List<CSVRecord>, peekSize: Int = 5): Boolean {
    try {
        records.take(peekSize).mapIndexed { rowIndex, csvRecord ->
            var cellValue = records[rowIndex][colName].toLowerCase()

            // remap some obvious ones
            cellValue = if (cellValue == "NA") "false" else cellValue // fixme add missing value support
            cellValue = if (cellValue == "F") "false" else cellValue
            cellValue = if (cellValue == "T") "true" else cellValue


            if (listOf("true", "false", "T", "F").contains(cellValue)) throw  NumberFormatException("invalid boolean cell value")

            return cellValue.toAsBoolNull() ?: true
        }

    } catch(e: NumberFormatException) {
        return false
    }

    return true
}


fun main(args: Array<String>) {
    val fromCSV = fromCSV("/Users/brandl/projects/kotlin/kplyr/src/test/resources/kplyr/data/msleep.csv")
    fromCSV.print()
    fromCSV.glimpse()
}