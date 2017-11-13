package krangl

import krangl.ArrayUtils.handleListErasure
import java.sql.ResultSet
import java.time.LocalDate
import java.time.LocalTime


// todo javadoc example needed
/** Create a data-frame from a list of objects */
fun <T> List<T>.asDataFrame(mapping: (T) -> DataFrameRow) = DataFrame.fromRecords(this, mapping)


/** Create a data-frame from a list of objects */
fun <T> DataFrame.Companion.fromRecords(records: List<T>, mapping: (T) -> DataFrameRow): DataFrame {
    val rowData = records.map { mapping(it) }
    val columnNames = mapping(records.first()).keys

    val columnData = columnNames.map { it to emptyList<Any?>().toMutableList() }.toMap()

    for (record in rowData) {
        columnData.forEach { colName, colData -> colData.add(record[colName]) }
    }

    return columnData.map { (name, data) -> handleListErasure(name, data) }.asDataFrame()
}

// todo javadoc example needed


/**
 * Create a new data frame in place.
 *
 * @sample krangl.samples.builderSample
 */
fun dataFrameOf(vararg header: String) = InplaceDataFrameBuilder(header.toList())

// added to give consistent api entrypoint


/**
 * Create a new data frame in place.
 *
 * @sample krangl.samples.builderSample
 */
fun DataFrame.Companion.builder(vararg header: String) = krangl.dataFrameOf(*header)


// tbd should we expose this as public API?
internal fun SimpleDataFrame.addColumn(dataCol: DataCol): SimpleDataFrame =
    SimpleDataFrame(cols.toMutableList().apply { add(dataCol) })


class InplaceDataFrameBuilder(private val header: List<String>) {


    operator fun invoke(vararg tblData: Any?): DataFrame {
        //        if(tblData.first() is Iterable<Any?>) {
        //            tblData = tblData.first() as Iterable<Any?>
        //        }


        // 1) break into columns
        val rawColumns: List<List<Any?>> = tblData.toList()
            .mapIndexed { i, any -> i.rem(header.size) to any }
            .groupBy { it.first }.values.map {
            it.map { it.second }
        }


        // 2) infer column type by peeking into column data
        val tableColumns = header.zip(rawColumns).map {
            handleListErasure(it.first, it.second)
        }

        require(tableColumns.map { it.length }.distinct().size == 1) {
            "Provided data does not coerce to tablular shape"
        }

        // 3) bind into data-frame
        return SimpleDataFrame(tableColumns)
    }


    //    operator fun invoke(values: List<Any?>): DataFrame {
    //        return invoke(values.toTypedArray())
    //    }

}


fun DataFrame.Companion.fromResultSet(rs: ResultSet): DataFrame {

    val numColumns = rs.metaData.columnCount
    val colNames = (1..numColumns).map { rs.metaData.getColumnName(it) }

    // see http://www.h2database.com/html/datatypes.html
    val colData = listOf<MutableList<Any?>>().toMutableList()

    val colTypes = (1..numColumns).map { rs.metaData.getColumnTypeName(it) }

    //    http://www.cs.toronto.edu/~nn/csc309/guide/pointbase/docs/html/htmlfiles/dev_datatypesandconversionsFIN.html
    colTypes.map {
        when (it) {
            "INTEGER", "INT", "SMALLINT" -> listOf<Int>()
            "REAL", "FLOAT", "NUMERIC", "DECIMAL" -> listOf<Double?>()
            "BOOLEAN" -> listOf<Boolean?>()
            "DATE" -> listOf<LocalDate?>()
            "TIME" -> listOf<LocalTime?>()
            "CHAR", "CHARACTER", "VARCHAR" -> listOf<String>()
            else -> throw IllegalArgumentException("Column type ${it} is not yet supported by {krangl}." +
                " Please file a ticket under https://github.com/holgerbrandl/krangl/issues")
        }.toMutableList().let { colData.add(it) }
    }

    // see https://stackoverflow.com/questions/21956042/mapping-a-jdbc-resultset-to-an-object
    while (rs.next()) {
        //        val row = mapOf<String, Any?>().toMutableMap()

        for (colIndex in 1..numColumns) {
            val any: Any? = when (colTypes[colIndex - 1]) {
                "INTEGER", "INT", "SMALLINT" -> rs.getInt(colIndex)
                "REAL", "FLOAT", "NUMERIC", "DECIMAL" -> rs.getDouble(colIndex)
                "BOOLEAN" -> rs.getBoolean(colIndex)
                "DATE" -> rs.getDate(colIndex).toLocalDate()
                "TIME" -> rs.getTime(colIndex).toLocalTime()
                "CHAR", "CHARACTER", "VARCHAR" -> rs.getString(colIndex)
                else -> throw IllegalArgumentException("Column type ${colTypes[colIndex - 1]} is not yet supported by {krangl}." +
                    " Please file a ticket under https://github.com/holgerbrandl/krangl/issues")
            }
            colData[colIndex - 1].add(any)
        }
    }
    return SimpleDataFrame(colNames.zip(colData).map { (name, data) -> handleListErasure(name, data) })

}