package krangl.experimental

import krangl.*

/**
 * Unfold all list columns vertically and all objects properties horizontally.
 * @author Holger Brandl
 */
fun DataFrame.flatten(): DataFrame {


    // first, unnest all list columns vertically
    val vertFlat = names.fold(this) { df, colName ->
        val firstVal = df[colName].values().asSequence().filterNotNull().firstOrNull()

        if (firstVal is List<*> || firstVal is DataFrame) df.unnest(colName) else df
    }


    // second, unfold object properties horizontally
    val vertHoriFlat = vertFlat.names.fold(vertFlat) { df, colName ->
        val firstVal = df[colName].values().asSequence().filterNotNull().firstOrNull()

        if (firstVal == null) return df

        // todo could refac unfold to work with Kclass parameter, because it's the only thing we have in here
        // and the type parameter is not even needed, because we can get the type from the first object in the column

        // may another flavor of unfold may also accept actual method references:     val kFunction1 = UUID::node
        //            val createInstance = firstVal::class.createInstance()
        //            val type = firstVal::class.javaObjectType

//        val type = firstVal::class.starProjectedType

        df.unfold<Any>(columnName = "dfo")
    }

    return vertHoriFlat
}


internal fun <T, K, L> cartesianProduct(left: Iterable<T>, right: Iterable<K>, transform: (T, K) -> L): Iterable<L> {
    return left.flatMap { someT -> right.map { someK -> transform(someT, someK) } }
}

// see https://youtrack.jetbrains.com/issue/KT-27028
internal object DoubleSum {

    @JvmStatic
    fun main(args: Array<String>) {
        val cartesianProduct = cartesianProduct(1..4, 4..20) { i, j -> 2 * i * j }.sum()
    }
}


object KranglOneHot {
    @JvmStatic
    fun main(args: Array<String>) {
        val oneHot = irisData.oneHot("Species")
        oneHot.schema()
    }
}


/**
 * Performs a one-hot encoding of the specified `Any`column.
 */
inline fun <reified T> DataFrame.oneHot(
        columnName: String,
        naValue: String = "NA",
        crossinline categorizeWith: (T?) -> String? = { it?.toString() }
): DataFrame {
//    val dataCol = this[columnName]
//    require(dataCol is AnyCol) { "only one-hot-encoding of string columns is supported at the moment." }

    return addColumn(columnName) { df ->
        df[columnName].map<T> { categorizeWith(it) ?: naValue }
    }.oneHot(columnName)
}


/**
 * Performs a one-hot encoding of the specified column.
 */
fun DataFrame.oneHot(columnName: String): DataFrame {
    val dataCol = this[columnName]
    require(dataCol is StringCol) { "only one-hot-encoding of string columns is supported at the moment." }

    // what about null

    val categories = dataCol.asStrings().distinct().filterNotNull()

    val hotCols: Map<String, IntArray> = categories.map { it to IntArray(nrow) }.toMap()

    dataCol.asStrings().mapIndexed { rowIndex, value ->
        hotCols[value]!![rowIndex] = 1
    }

    val oneHotCols = hotCols.map { (name, data) -> IntCol("$columnName[$name]", data) }

    return bindCols(this.remove(columnName), dataFrameOf(*oneHotCols.toTypedArray()))
}

