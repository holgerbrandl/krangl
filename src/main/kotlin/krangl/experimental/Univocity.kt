package krangl.experimental

/**
 * @author Holger Brandl
 */

import com.univocity.parsers.tsv.TsvParser
import com.univocity.parsers.tsv.TsvParserSettings
import krangl.*
import java.io.BufferedReader
import java.io.Reader

/**
 * @author Holger Brandl
 */

// https://github.com/uniVocity/csv-parsers-comparison#jdk-6-1

abstract class CellParser {
    val dataBuffer: ArrayList<Any> = ArrayList<Any>(1E6.toInt())

    abstract fun parseCell(value: String): Any
    abstract fun makeColumn(name: String): DataCol
}

internal fun TsvParser.iterator() = object : AbstractIterator<Array<String>>() {
    override fun computeNext() {
        val parseNext = parseNext()
        if (parseNext != null) setNext(parseNext) else done()
    }
}


fun DataFrame.Companion.readTsvUnivox(
    reader: Reader,
    settings: TsvParserSettings = TsvParserSettings().apply { isHeaderExtractionEnabled = true },
    colTypes: Map<String, ColType> = mapOf()
): DataFrame {

    // from https://www.univocity.com/pages/univocity_parsers_tutorial
    //    val settings = TsvParserSettings()
    //the file used in the example uses '\n' as the line separator sequence.
    //the line separator sequence is defined here to ensure systems such as MacOS and Windows
    //are able to process this file correctly (MacOS uses '\r'; and Windows uses '\r\n').

//    settings.setLineSeparator("\n")

    // creates a CSV parser
    val parser = TsvParser(settings)

    settings.nullValue = MISSING_VALUE
//    val formatWithNullString = if (settings.nullValue != null) {
//        settings
//    } else {
//        settings.nullValue = MISSING_VALUE
//        settings
//    }

    val peakBufferSize = 100
    val peakBuffer = listOf<Array<String>>().toMutableList()

    parser.beginParsing(BufferedReader(reader))
    val rowIterator = parser.iterator()

    (0..peakBufferSize).map { if (rowIterator.hasNext()) peakBuffer.add(rowIterator.next()) }


    // build names
    val header = peakBuffer.first()

    val columnNames = if (settings.isHeaderExtractionEnabled) header.toList() else
        header.mapIndexed { index, value -> "X${index}" }

    // Make column names unique when reading them + unit test
    val uniqueNames = columnNames
        .withIndex()
        .groupBy { it.value }
        .flatMap { (grpName, columns) ->
            columns
                .mapIndexed { index, indexedValue ->
                    indexedValue.index to (grpName + if (index > 0) "_${index + 1}" else "")
                }
        }
        .sortedBy { it.first }.map { it.second }

    //    csvParser.headerMap.keys.pmap{colName ->
    val recordsNoHeader = if (settings.isHeaderExtractionEnabled) peakBuffer.drop(1) else peakBuffer

    // detect types
    val parsers: Array<CellParser> = uniqueNames.mapIndexed { colIndex, colName ->
        val defaultColType = colTypes[".default"] ?: ColType.Guess

        val colType = colTypes[colName] ?: defaultColType

        buildParserMap(colName, colIndex, colType, recordsNoHeader)
    }.toTypedArray()


    // parse data

    // 1. the peek buffer
    parseRecords(parsers, recordsNoHeader.iterator())

    // 2. the rest
    parseRecords(parsers, rowIterator)

    val cols = parsers.zip(uniqueNames).map { (parser, name) ->
        parser.makeColumn(name)

    }

    return SimpleDataFrame(cols)
}

fun parseRecords(parsers: Array<CellParser>, records: Iterator<Array<String>>) {
    val parserIndices = (0..(parsers.size - 1)).toList()
    for (line in records) {
        for (colIndex in parserIndices) {
            if (line.size <= colIndex) {
                println("too many elements ${line.size} vs ${colIndex}")
                continue
            }
            parsers[colIndex].parseCell(line[colIndex])
        }
    }
}


fun ColType.makeParser(): CellParser = when (this) {

    ColType.Int -> object : CellParser() {
        override fun makeColumn(name: String): DataCol = IntCol(name, dataBuffer as List<Int?>)

        override fun parseCell(value: String): Any = dataBuffer.add(value.toInt())
    }

    ColType.Double -> object : CellParser() {
        override fun makeColumn(name: String): DataCol = DoubleCol(name, dataBuffer as List<Double?>)

        override fun parseCell(value: String): Any = dataBuffer.add(value.toDouble())
    }

    ColType.Boolean -> object : CellParser() {
        override fun makeColumn(name: String): DataCol = BooleanCol(name, dataBuffer as List<Boolean?>)

        override fun parseCell(value: String): Any = dataBuffer.add(value.toBoolean())
    }

    ColType.String -> object : CellParser() {
        override fun makeColumn(name: String): DataCol = StringCol(name, dataBuffer as List<String?>)

        override fun parseCell(value: String): Any = dataBuffer.add(value)
    }

    ColType.Guess -> TODO("should not happen")
}

internal fun buildParserMap(colName: String, colIndex: Int, colType: ColType, records: List<Array<String>>): CellParser =

    when (colType) {
        ColType.Guess -> buildParserMap(colName, colIndex, guessColType(peekColUniVox(colIndex, records)), records)
        else -> colType.makeParser()
    }


internal fun peekColUniVox(colIndex: Int, records: List<Array<String>>, peekSize: Int = 10) = records
    .asSequence()
    .mapIndexed { rowIndex, _ -> records[rowIndex][colIndex] }
    .filterNotNull()
    .take(peekSize)
    .toList()