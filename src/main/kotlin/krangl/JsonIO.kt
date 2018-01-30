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

fun DataFrame.Companion.fromJson(url: URL): DataFrame =
    fromJsonArray(Parser().parse(url.openStream()) as JsonArray<JsonObject>)


fun DataFrame.Companion.fromJsonString(jsonData: String) =
    fromJsonArray(Parser().parse(StringReader(jsonData)) as JsonArray<JsonObject>)


// todo implement proper flattening here
internal fun fromJsonArray(records: JsonArray<JsonObject>): DataFrame {
    //    records[0]["sdf"]
    val columnNames = records.first().keys.toList()
    //    parse.map { it.keys }
    //
    fun asColumn(colName: String, records: JsonArray<JsonObject>, builder: () -> DataCol): DataCol {
        return try {
            builder()
        } catch (e: NumberFormatException) {
            StringCol(colName, records.map { it[colName] as String? })
        } catch (e: ClassCastException) {
            StringCol(colName, records.map { it[colName] as String? })
        }
    }

    val cols = columnNames.map { colName ->
        val firstElements = records.take(5).mapIndexed { rowIndex, _ -> records[rowIndex][colName] }

        try {
            when {
            // see https://github.com/holgerbrandl/krangl/issues/10
                firstElements.all { it is Int? || it is Long? } -> IntCol(colName, records.map { (it[colName] as Number?)?.toInt() })
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