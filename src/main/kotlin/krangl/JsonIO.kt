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

@Suppress("UNCHECKED_CAST")
fun DataFrame.Companion.fromJson(url: URL): DataFrame =
        fromJsonArray(Parser().parse(url.openStream()) as JsonArray<JsonObject>)


@Suppress("UNCHECKED_CAST")
fun DataFrame.Companion.fromJsonString(jsonData: String): DataFrame {
    val parsed = Parser().parse(StringReader(jsonData))

    //    var deparseJson = deparseJson(parsed)
    val ARRAY_COL_ID = "_id"
    var df = dataFrameOf(ARRAY_COL_ID)(parsed)

    fun isJsonColumn(it: DataCol) = getColumnType(it).startsWith("Json")

    // convert all json columns
    while (df.cols.any { isJsonColumn(it) }) {
        //        df.schema()
        val jsonCol = df.cols.first { isJsonColumn(it) }

        val jsonColDFs = jsonCol.values().map {
            when (it) {
                is JsonArray<*> -> fromJsonArray(it as JsonArray<JsonObject>)
                is JsonObject -> when {
                    it.values.first() is JsonArray<*> -> {
                        dataFrameOf(StringCol(jsonCol.name, it.keys.toList()), AnyCol("value", it.values.toList()))
                    }
                    else -> {
                        it.toMap().map { (key, value) -> AnyCol(key, listOf(value)) }
                                .let { dataFrameOf(*it.toTypedArray()) }.addColumn(ARRAY_COL_ID) { it.df.names }
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


private fun deparseJson(parsed: Any?): DataFrame {
    @Suppress("UNCHECKED_CAST")
    return when (parsed) {
        is JsonArray<*> -> fromJsonArray(parsed as JsonArray<JsonObject>)
        is JsonObject -> dataFrameOf(parsed.keys)(parsed.values)
        else -> throw IllegalArgumentException("Can not parse json. " + INTERNAL_ERROR_MSG)
    }
}


internal fun fromJsonArray(records: JsonArray<JsonObject>): DataFrame {
    //    records[0]["sdf"]
    val colNames = records
            .map { it.keys.toList() }
            .reduceRight { acc, right -> acc + right.minus(acc) }

    fun asColumn(colName: String, records: JsonArray<JsonObject>, builder: () -> DataCol): DataCol {
        return try {
            builder()
        } catch (e: NumberFormatException) {
            StringCol(colName, records.map { it[colName] as String? })
        } catch (e: ClassCastException) {
            StringCol(colName, records.map { it[colName] as String? })
        }
    }

    val cols = colNames.map { colName ->
        val firstElements = records.take(5).mapIndexed { rowIndex, _ -> records[rowIndex][colName] }

        try {
            when {
                // see https://github.com/holgerbrandl/krangl/issues/10
                firstElements.all { it is Int? || it is Long? } -> IntCol(colName, records.map { (it[colName] as Number?)?.toInt() })
                firstElements.all { it is Long? || it is Long? } -> LongCol(colName, records.map { (it[colName] as Number?)?.toLong() })
                firstElements.all { it is Double? } -> DoubleCol(colName, records.map { (it[colName] as Number?)?.toDouble() })
                firstElements.all { it is Boolean? } -> BooleanCol(colName, records.map { it[colName] as Boolean? })
                firstElements.all { it is Boolean? } -> BooleanCol(colName, records.map { it[colName] as Boolean? })
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


fun main(args: Array<String>) {
    DataFrame.fromJson("https://raw.githubusercontent.com/vega/vega/master/test/data/movies.json")
}