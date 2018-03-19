package krangl

import krangl.ArrayUtils.handleListErasure
import krangl.util.asDF
import java.sql.ResultSet
import java.time.LocalDate
import java.time.LocalTime
import kotlin.reflect.KProperty
import kotlin.reflect.full.declaredMembers


// todo javadoc example needed
/** Create a data-frame from a list of objects */
fun <T> Iterable<T>.deparseRecords(mapping: (T) -> DataFrameRow) = DataFrame.fromRecords(this, mapping)


/** Create a data-frame from a list of objects */
fun <T> DataFrame.Companion.fromRecords(records: Iterable<T>, mapping: (T) -> DataFrameRow): DataFrame {
    val rowData = records.map { mapping(it) }
    val columnNames = mapping(records.first()).keys

    val columnData = columnNames.map { it to emptyList<Any?>().toMutableList() }.toMap()

    for (record in rowData) {
        columnData.forEach { colName, colData -> colData.add(record[colName]) }
    }

    return columnData.map { (name, data) -> handleListErasure(name, data) }.asDF()
}


/**
 * Turn a list of objects into a data-frame using reflection. Currently just properties without any nesting are supported.
 */
inline fun <reified T> Iterable<T>.asDataFrame(): DataFrame {
    val declaredMembers = T::class.declaredMembers
    //    declaredMembers.first().call(this[0])

    val properties = T::class.declaredMembers
        .filter { it.parameters.toList().size == 1 }
        .filter { it is KProperty }

    val results = properties.map {
        it.name to this.map { el -> it.call(el) }
    }

    val columns = results.map { handleListErasure(it.first, it.second) }

    return columns.asDF()
}




/** Convert rows into objects by using reflection. Only parameters used in constructor will be mapped.
 * Note: This is tested with kotlin data classes only. File a ticket for better type compatiblity or any issues!
 * @param mapping parameter mapping scheme to link data frame columns to object properties mapOf("someName" to "lastName", etc.)
 */
inline fun <reified T> DataFrame.rowsAs(mapping: Map<String, String> = names.map { it to it }.toMap()): Iterable<T> {

    // for each constructor check the best matching one and create an object accordingly
    val constructors = T::class.constructors

    require(names.containsAll(mapping.keys)) { "Mapping columns ${mapping.keys.minus(names)} missing in data frame!" }

    // append names to mapping table
    val dummyMap = names.map { it to it }.toMap()
    val varLookup = dummyMap.minus(mapping.keys) + mapping

    val bestConst = constructors
        // just constructors for which all columns are present
        .filter { varLookup.values.containsAll(it.parameters.map { it.name }) }
        // select the one with most parameters
        .maxBy { it.parameters.size }

    if (bestConst == null) error("[krangl] Could not find matching constructor for subset of ${mapping.values}")

    val objects = rows.map { row ->
        val args = bestConst.parameters.map { row[varLookup[it.name]] }
        bestConst.call(*args.toTypedArray())
    }

    return objects
}


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


    operator fun invoke(args: Iterable<Any?>): DataFrame {
        return invoke(*args.toList().toTypedArray())
    }


    operator fun invoke(args: Sequence<Any?>): DataFrame {
        return invoke(*args.toList().toTypedArray())
    }

    operator fun invoke(vararg tblData: Any?): DataFrame {
        //        if(tblData.first() is Iterable<Any?>) {
        //            tblData = tblData.first() as Iterable<Any?>
        //        }

        // is the data vector compatible with the header dimension?
        require(header.size > 0 && tblData.size.rem(header.size) == 0) {
            "data dimension ${header.size} is not compatible with length of data vector ${tblData.size}"
        }

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
            "Provided data does not coerce to tabular shape"
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