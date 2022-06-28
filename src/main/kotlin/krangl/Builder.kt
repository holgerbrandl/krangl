package krangl

import krangl.ArrayUtils.handleListErasure
import krangl.util.asDF
import krangl.util.createValidIdentifier
import krangl.util.detectPropertiesByReflection
import java.sql.ResultSet
import java.sql.Types
import java.time.LocalDate
import java.time.LocalTime
import kotlin.reflect.KCallable
import kotlin.reflect.KProperty


// todo javadoc example needed
/** Create a data-frame from a list of objects */
fun <T> Iterable<T>.deparseRecords(mapping: (T) -> DataFrameRow) = DataFrame.fromRecords(this, mapping)

internal typealias DeparseFormula<T> = T.(T) -> Any?

inline fun <reified T> Iterable<T>.deparseRecords(vararg mapping: Pair<String, DeparseFormula<T>>): DataFrame {
    //    val revMapping = mapping.toMap().entries.associateBy({ it.value }) { it.key }
    val mappings = mapOf<String, Any?>().toMutableMap().apply { putAll(mapping) }

    val function = { record: T ->
        mapping.toMap().map { (name, deparse) -> name to deparse(record, record) }.toMap() as DataFrameRow
    }
    return DataFrame.fromRecords(this, function)
}


infix fun <T> String.with(that: DeparseFormula<T>) = Pair(this, that)


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
    // val declaredMembers = T::class.declaredMembers
    //    declaredMembers.first().call(this[0])

    val members = T::class.members

    val properties = members
        .filter { it.parameters.toList().size == 1 }
        .filter { it is KProperty }

    // todo call kotlin getters
    //  declaredMembers.toList()[1].call(first())

    // todo call regular getters


    val results = properties.map {
        it.name to this.map { el -> it.call(el) }
    }

    val columns = results.map { handleListErasure(it.first, it.second) }


    return columns.asDF()
}



//typealias PropExtractor<T> = T.(T) -> Any?
typealias PropExtractor<T> = T.() -> Any?


fun main() {
    data class Car(val name:String, val weight: Double)

    val cars = listOf(Car("BMW", 123.0), Car("Tesla", 245.0))
    val df = dataFrameOf(AnyCol("car", cars))
    df.unfold("car", listOf(Car::name,Car::weight, Car::weight))
}

@JvmName("unfoldByProperty")
fun  DataFrame.unfold(
    columnName: String,
    properties: List<KCallable<*>>,
    keep: Boolean = true,
    addPrefix:Boolean=false
): DataFrame {

    // todo make sure that unfolded columns are not yet present in df, and warn if so and append _1, _2, ... suffix
    val unfolded = properties.fold(this) { df, kCallable ->
        val colName = if(addPrefix) columnName + "_" + kCallable.name else kCallable.name

        df.addColumn(colName) {
            df[columnName].map<Any> {
                kCallable.call(it)
            }
        }
    }

    return if (keep) unfolded else unfolded.remove(columnName)
}


inline fun <reified T> DataFrame.unfold(
    columnName: String,
    properties: List<String> = detectPropertiesByReflection<T>().map { it.name },
    keep: Boolean = true,
    addPrefix:Boolean=false
): DataFrame {

    val extProperties = properties + properties.map { "get" + it.capitalize() }
    val propsOrGetters = detectPropertiesByReflection<T>()

    val filtMembers = propsOrGetters
        // match by name
        .filter { it.parameters.size==1 && extProperties.contains(it.name) }  // discard extension functions in class body

    // todo make sure that unfolded columns are not yet present in df, and warn if so and append _1, _2, ... suffix
    val unfolded = filtMembers.fold(this) { df, kCallable ->
        val colName = if(addPrefix) columnName + "_" + kCallable.name else kCallable.name

        df.addColumn(colName) {
            df[columnName].map<T> {
                kCallable.call(it)
            }
        }
    }

    return if (keep) unfolded else unfolded.remove(columnName)
}


/** Convert rows into objects by using reflection. Only parameters used in constructor will be mapped.
 * Note: This is tested with kotlin data classes only. File a ticket for better type compatibility or any issues!
 * @param mapping parameter mapping scheme to link data frame columns to object properties mapOf("someName" to "lastName", etc.)
 */
inline fun <reified T> DataFrame.rowsAs(mapping: Map<String, String> = names.map { it to it }.toMap()): Iterable<T> {

    // for each constructor check the best matching one and create an object accordingly
    val constructors = T::class.constructors

    require(names.containsAll(mapping.keys)) { "Mapping columns ${mapping.keys.minus(names)} missing in data frame!" }

    // append names to mapping table
    val dummyMap = names.map { it to it }.toMap()
    val varLookup = (dummyMap.minus(mapping.keys) + mapping).entries.associateBy({ it.value }) { it.key }

    // finally we use the same recoding strategy as in printDataClassSchema to create legit identifiers
    val legitIdentLookup = varLookup.keys.map { createValidIdentifier(it) to it }.toMap()


    val bestConst = constructors
        // just constructors for which all columns are present
        .filter { legitIdentLookup.keys.containsAll(it.parameters.map { it.name }) }
        // select the one with most parameters
        .maxByOrNull { it.parameters.size }

    if (bestConst == null) error("[krangl] Could not find matching constructor for subset of ${mapping.values}")

    val objects = rows.map { row ->
        val args = bestConst.parameters.map { constParamName ->
            row[varLookup[legitIdentLookup[constParamName.name]]]
        }

        try {
            bestConst.call(*args.toTypedArray())
        } catch (e: IllegalArgumentException) {
            throw IllegalArgumentException("Could not instantiate record class constructor $bestConst with $args")
        }
    }

    return objects
}


/**
 * Create a new data frame in place.
 *
 * @sample krangl.samples.builderSample
 */
fun dataFrameOf(vararg header: String) = InplaceDataFrameBuilder(header.toList())


/**
 * Create a new data frame in place.
 *
 * @sample krangl.samples.builderSample
 */
fun dataFrameOf(header: Iterable<String>) = InplaceDataFrameBuilder(header.toList())


/**
 * Create a new data-frame from a list of `DataCol` instances
 *
 * @sample krangl.samples.builderSample
 */
fun dataFrameOf(vararg columns: DataCol): DataFrame = SimpleDataFrame(*columns)


/** Create a new data-frame from a records encoded as key-value maps.
 *
 * Column types will be inferred from the value types.
 * @sample krangl.samples.builderSample
 */
fun dataFrameOf(rows: Iterable<DataFrameRow>): DataFrame {
    val colNames = rows.first().keys

    return colNames.map { colName ->
        val colData = rows.map { it[colName] }
        handleListErasure(colName, colData)
    }.let { dataFrameOf(*it.toTypedArray()) }
}


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

        // https://github.com/holgerbrandl/krangl/issues/125
        require(rawColumns.isNotEmpty()) { "Can not infer column types in empty data-frame. " +
                "To create an empty data-frame use the following syntax dataFrameOf(StringCol(\"user\", emptyArray()), DoubleCol(\"salary\", emptyArray()))"
        }

//        require(rawColumns.isEmpty() || tableColumns.map { it.length }.distinct().size == 1) {
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

    val colTypes = (1..numColumns).map { rs.metaData.getColumnType(it) }

    //    http://www.cs.toronto.edu/~nn/csc309/guide/pointbase/docs/html/htmlfiles/dev_datatypesandconversionsFIN.html
    colTypes.map {
        when (it) {
            Types.INTEGER, Types.SMALLINT -> listOf<Int>()
            Types.BIGINT -> listOf<Long>()
            Types.DECIMAL, Types.FLOAT, Types.NUMERIC,Types.REAL,Types.DOUBLE -> listOf<Double?>()
            Types.BOOLEAN -> listOf<Boolean?>()
            Types.DATE, Types.TIMESTAMP -> listOf<LocalDate?>()
            Types.TIME -> listOf<LocalTime?>()
            Types.CHAR, Types.LONGVARCHAR, Types.VARCHAR, Types.NVARCHAR -> listOf<String>()
            else -> throw IllegalArgumentException("Column type ${it} is not yet supported by {krangl}. $PLEASE_SUBMIT_MSG")
        }.toMutableList<Any?>().let { colData.add(it) }
    }

    // see https://stackoverflow.com/questions/21956042/mapping-a-jdbc-resultset-to-an-object
    while (rs.next()) {
        //        val row = mapOf<String, Any?>().toMutableMap()
        for (colIndex in 1..numColumns) {
            val any: Any? = when (colTypes[colIndex - 1]) {
                Types.INTEGER, Types.SMALLINT -> rs.getInt(colIndex)
                Types.BIGINT  -> rs.getLong(colIndex)
                Types.DECIMAL, Types.FLOAT, Types.NUMERIC,Types.REAL,Types.DOUBLE -> rs.getDouble(colIndex)
                Types.BOOLEAN -> rs.getBoolean(colIndex)
                Types.DATE,   -> rs.getDate(colIndex).toLocalDate()
                Types.TIMESTAMP -> rs.getTimestamp(colIndex).toLocalDateTime()
                Types.TIME -> rs.getTime(colIndex).toLocalTime()
                Types.CHAR, Types.LONGVARCHAR, Types.VARCHAR, Types.NVARCHAR -> rs.getString(colIndex)
                else -> throw IllegalArgumentException("Column type ${colTypes[colIndex - 1]} is not yet supported by {krangl}. $PLEASE_SUBMIT_MSG")
            }
            colData[colIndex - 1].add(any)
        }
    }
    return SimpleDataFrame(colNames.zip(colData).map { (name, data) -> handleListErasure(name, data) })

}