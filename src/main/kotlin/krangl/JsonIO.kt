package krangl

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import java.io.File
import java.io.StringReader
import java.net.URL

/**
 * @author Holger Brandl
 */

fun DataFrame.Companion.fromJson(file: File) = fromJson(file.toURI().toURL())

// convenience impl that guesses typoe of input
fun DataFrame.Companion.fromJson(fileOrUrl: String): DataFrame {
    // val fileOrUrl = "https://raw.githubusercontent.com/vega/vega/master/test/data/movies.json"
    val url = (if (isURL(fileOrUrl)) {
        URL(fileOrUrl).toURI()
    } else {
        File(fileOrUrl).toURI()
    }).toURL()

    return fromJson(url)
}

const val ARRAY_ROWS_TYPE_DETECTING = 5

@Suppress("UNCHECKED_CAST")
fun DataFrame.Companion.fromJson(url: URL, typeDetectingRows: Int? = ARRAY_ROWS_TYPE_DETECTING): DataFrame =
    fromJsonArray(Parser.default().parse(url.openStream()) as JsonArray<JsonObject>, typeDetectingRows)

const val ARRAY_COL_ID = "_id"

@Suppress("UNCHECKED_CAST")
fun DataFrame.Companion.fromJsonString(jsonData: String, typeDetectingRows: Int? = ARRAY_ROWS_TYPE_DETECTING): DataFrame {
    val parsed = Parser.default().parse(StringReader(jsonData))

    //    var deparseJson = deparseJson(parsed)
    var df = dataFrameOf(ARRAY_COL_ID)(parsed)

    fun isJsonColumn(it: DataCol) = getColumnType(it).startsWith("Json")

    // convert all json columns
    while (df.cols.any { isJsonColumn(it) }) {
        //        df.schema()
        val jsonCol = df.cols.first { isJsonColumn(it) }

        val jsonColDFs = jsonCol.values().map { colData ->
            when (colData) {
                is JsonArray<*> -> fromJsonArray(colData as JsonArray<JsonObject>, typeDetectingRows)
                is JsonObject -> when {
                    colData.values.first() is JsonArray<*> -> {
                        dataFrameOf(
                            StringCol(jsonCol.name, colData.keys.toList()),
                            AnyCol("value", colData.values.toList())
                        )
                    }
                    else -> {
                        colData.toMap().map { (key, value) -> ArrayUtils.handleListErasure(key, listOf(value)) }
                            .let { dataFrameOf(*it.toTypedArray()) }
//                            .addColumn("_array") { it.df.names }
                    }
                }
                //                is JsonObject -> dataFrameOf(StringCol(jsonCol.name, it.keys.toList()), AnyCol("value", it.values.toList()))
                //                    dataFrameOf(StringCol(jsonCol.name, it.keys.toList()), AnyCol("value", it.values.toList()))
                else -> throw IllegalArgumentException("Can not parse json. " + INTERNAL_ERROR_MSG)
            }
        }

        // preserve the root id
        //        deparseJson = if (deparseJson.ncol == 1) dataFrameOf(ARRAY_COL_ID)(deparseJson.names) else deparseJson

        df = df
            .addColumn("_dummy_") { null }
            .remove(jsonCol.name)
            .addColumn("_json_") { jsonColDFs }
            .remove("_dummy_")
            .unnest("_json_")

    }

    return df
}

//Can this be removed?
private fun deparseJson(parsed: Any?, typeDetectingRows: Int? = ARRAY_ROWS_TYPE_DETECTING): DataFrame {
    @Suppress("UNCHECKED_CAST")
    return when (parsed) {
        is JsonArray<*> -> fromJsonArray(parsed as JsonArray<JsonObject>, typeDetectingRows)
        is JsonObject -> dataFrameOf(parsed.keys)(parsed.values)
        else -> throw IllegalArgumentException("Can not parse json. " + INTERNAL_ERROR_MSG)
    }
}


internal fun fromJsonArray(records: JsonArray<JsonObject>, typeDetectingRows: Int?): DataFrame {
    val colNames = records
        .map { it.keys.toList() }
        .reduceRight { acc, right -> acc + right.minus(acc) }

    val cols = colNames.map { colName ->
        val firstRows = if (typeDetectingRows is Int) {
            records.take(typeDetectingRows)
        } else {
            records
        }
        val firstElements = firstRows.mapIndexed { rowIndex, _ -> records[rowIndex][colName] }

        try {
            when {
                // see https://github.com/holgerbrandl/krangl/issues/10
                firstElements.all { it is Int? } -> IntCol(colName, records.map { (it[colName] as Number?)?.toInt() })
                firstElements.all { it is Long? } -> LongCol(colName, records.map { (it[colName] as Number?)?.toLong() })
                firstElements.all { it is Double? } -> DoubleCol(colName, records.map { (it[colName] as Number?)?.toDouble() })
                firstElements.all { it is Boolean? } -> BooleanCol(colName, records.map { it[colName] as Boolean? })
                //keep common numeric type
                firstElements.all { it is Int? || it is Long? } -> LongCol(colName, records.map { (it[colName] as Number?)?.toLong() })
                firstElements.all { it is Int? || it is Long? || it is Double? } -> DoubleCol(colName, records.map { (it[colName] as Number?)?.toDouble() })
                else -> StringCol(colName, records.map { it[colName]?.toString() })
            }
        } catch (e: NumberFormatException) {
            StringCol(colName, records.map { it[colName] as String? })
        } catch (e: ClassCastException) {
            StringCol(colName, records.map { it[colName] as String? })
        }
    }

    return SimpleDataFrame(cols)
}

fun DataFrame.toJsonArray(): JsonArray<JsonObject> {
    val jsonArray = JsonArray<JsonObject>()
    this.rows.map { row ->
        val jsonRow = JsonObject()
        row.map { element ->
            jsonRow[element.key] = element.value
        }
        jsonArray.add(jsonRow)
    }
    return jsonArray
}

fun DataFrame.toJsonObject(): JsonObject {
    val unnamedIndex = this.names.contains(ARRAY_COL_ID)
    val indexColumn = if (unnamedIndex) {
        this[ARRAY_COL_ID]
    } else {
        cols.first()
    }
    val indexName = indexColumn.name
    val index = indexColumn.values().distinct()
    val jsonObject = JsonObject()

    for (indexKey in index) {
        val indexValues = dataFrameOf(this.rows.filter { it[indexName] == indexKey }).remove(indexName)
        jsonObject[indexKey.toString()] = indexValues.toJsonArray()
    }
    return if (unnamedIndex) {
        jsonObject
    } else {
        JsonObject(mapOf(indexName to jsonObject))
    }
}

fun DataFrame.toJsonString(prettyPrint: Boolean = false, asObject: Boolean = false): String {
    val json = if (asObject) {
        toJsonObject()
    } else {
        toJsonArray()
    }
    return json.toJsonString(prettyPrint, false)
}

fun main(args: Array<String>) {
    DataFrame.fromJson("https://raw.githubusercontent.com/vega/vega/master/test/data/movies.json")
}
