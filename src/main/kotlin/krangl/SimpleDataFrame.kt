package krangl

import krangl.ArrayUtils.handleArrayErasure
import krangl.ArrayUtils.handleListErasure
import krangl.util.createComparator
import java.util.*


internal class SimpleDataFrame(override val cols: List<DataCol>) : DataFrame {

    // potential performance bottleneck when processing many-groups/joins
    init {
        // validate input columns
        cols.map { it.name }.let {
            if (it.distinct().size != it.size) throw DuplicatedColumnNameException(it)
        }
    }


    // deprecated indexed row access
    @Deprecated("use news zip-iterator instead")
    private val rowsIndexded = object : Iterable<DataFrameRow> {

        override fun iterator() = object : Iterator<DataFrameRow> {
            var curRow = 0

            override fun hasNext(): Boolean = curRow < nrow

            override fun next(): DataFrameRow = row(curRow++)
        }
    }


    private data class ColIterator(val name: String, val iterator: Iterator<Any?>)

    //    override val rows = object : Iterable<DataFrameRow> {
    //
    //        override fun iterator() = object : Iterator<DataFrameRow> {
    //
    //            val colIterators = cols.map { it.values().iterator() }.zip(names).map { ColIterator(it.second, it.first) }
    //
    //            override fun hasNext(): Boolean = colIterators.first().iterator.hasNext()
    //
    //            override fun next(): DataFrameRow = colIterators.map { it.name to it.iterator.next() }.toMap()
    //        }
    //    }

    override val rows = object : Iterable<DataFrameRow> {

        override fun iterator() = object : Iterator<DataFrameRow> {

            val colIterators = rowData().iterator()

            override fun hasNext(): Boolean = colIterators.hasNext()

            override fun next(): DataFrameRow = names.zip(colIterators.next()).toMap()
        }
    }


    //    override val raw = object : Iterable<DataFrameRow> {


    override fun select(vararg columns: String): DataFrame {
        warnIf(columns.isEmpty() &&
                // it may happen that internally we do an empty selection. e.g when joining a df on all columns with itself.
                // to prevent misleading logging we check for that by detecting the context of this call
                !Thread.currentThread().getStackTrace().map { it.methodName }.contains("join")
        ) {
            "Calling select() will always return an empty data-frame"
        }

        require(names.containsAll(columns.asList())) { "not all selected columns (${columns.joinToString(", ")}) are contained in table" }
        require(columns.distinct().size == columns.size) { "Columns must not be selected more than once" }

        return columns.fold(SimpleDataFrame(), { df, colName -> df.addColumn(this[colName]) })
    }

    // Utility methods

    override fun row(rowIndex: Int): DataFrameRow =
        cols.map {
            it.name to when (it) {
                is DoubleCol -> it.values[rowIndex]
                is IntCol -> it.values[rowIndex]
                is LongCol -> it.values[rowIndex]
                is BooleanCol -> it.values[rowIndex]
                is StringCol -> it.values[rowIndex]
                is AnyCol -> it.values[rowIndex]
                else -> throw UnsupportedOperationException()
            }
        }.toMap()

    override val ncol = cols.size


    override val nrow by lazy {
        val firstCol: DataCol? = cols.firstOrNull()
        when (firstCol) {
            null -> 0
            is DoubleCol -> firstCol.values.size
            is IntCol -> firstCol.values.size
            is LongCol -> firstCol.values.size
            is BooleanCol -> firstCol.values.size
            is StringCol -> firstCol.values.size
            is AnyCol -> firstCol.values.size
            else -> throw UnsupportedOperationException()
        }
    }


    /** This method is private to enforce use of mutate which is the primary way to add columns in krangl. */
    private fun addColumn(newCol: DataCol): SimpleDataFrame {
        // make sure that table is either empty or row number matches table row count
        require(nrow == 0 || newCol.length == nrow) { "Column lengths of dataframe ($nrow) and new column (${newCol.length}) differ" }
        require(newCol.name !in names) { "Column '${newCol.name}' already exists in dataframe" }
        require(newCol.name != tempColumnName()) { "Internal temporary column name should not be expose to user" }

        val mutatedCols = cols.toMutableList().apply { add(newCol) }
        return SimpleDataFrame(mutatedCols.toList())
    }

    /** Returns the ordered list of column names of this data-frame. */
    override val names: List<String> = cols.map { it.name }


    override operator fun get(columnName: String): DataCol = try {
        cols.first { it.name == columnName }
    } catch (e: NoSuchElementException) {
        throw NoSuchElementException("Could not find column '${columnName}' in dataframe")
    }

    // Core Verbs

    override fun filter(predicate: VectorizedRowPredicate): DataFrame {
        val indexFilter = predicate(this.ec, this.ec)

        require(indexFilter.size == nrow) { "filter index has incompatible length" }

        return cols.map {
            // subset a colum by the predicate array
            when (it) {
                is DoubleCol -> DoubleCol(
                    it.name,
                    it.values.filterIndexed { index, _ -> indexFilter[index] }.toTypedArray()
                )
                is IntCol -> IntCol(it.name, it.values.filterIndexed { index, _ -> indexFilter[index] }.toTypedArray())
                is LongCol -> LongCol(
                    it.name,
                    it.values.filterIndexed { index, _ -> indexFilter[index] }.toTypedArray()
                )
                is StringCol -> StringCol(
                    it.name,
                    it.values.filterIndexed { index, _ -> indexFilter[index] }.toList().toTypedArray()
                )
                is BooleanCol -> BooleanCol(
                    it.name,
                    it.values.filterIndexed { index, _ -> indexFilter[index] }.toList().toTypedArray()
                )
                is AnyCol -> AnyCol(
                    it.name,
                    it.values.filterIndexed { index, _ -> indexFilter[index] }.toList().toTypedArray()
                )
                else -> throw UnsupportedOperationException()
            }
        }.let { SimpleDataFrame(it) }
    }


    // also provide vararg constructor for convenience
    constructor(vararg cols: DataCol) : this(cols.asList())

    override fun summarize(vararg sumRules: ColumnFormula): DataFrame {
        //        require(nrow > 0) { "Can not summarize empty data-frame" } // todocan dplyr?
        /**
        require(dplyr)
        data_frame() %>% summarize(test=1)
         */

        val sumCols = mutableListOf<DataCol>()
        for ((key, sumRule) in sumRules) {
            val sumValue = sumRule(this.ec, this.ec)
            when (sumValue) {
                is Int -> IntCol(key, arrayOf(sumValue as Int?))
                is Long -> LongCol(key, arrayOf(sumValue as Long?))
                is Double -> DoubleCol(key, arrayOf(sumValue as Double?))
                is Boolean -> BooleanCol(key, arrayOf(sumValue as Boolean?))
                is String -> StringCol(key, arrayOf(sumValue as String?))

                // prevent non-scalar summaries. See krangl/test/CoreVerbsTest.kt:165
                is DataCol -> throw NonScalarValueException(ColumnFormula(key, sumRule), sumValue)
                is IntArray, is BooleanArray, is DoubleArray, is FloatArray -> throw NonScalarValueException(
                    ColumnFormula(key, sumRule),
                    "Array"
                )
                is Iterable<*> -> throw NonScalarValueException(ColumnFormula(key, sumRule), "List")

                // todo does null-handling makes sense at all? Might be not-null in other groups for grouped operations // todo add unit-test
                //                null -> AnyCol(key, listOf(null)) // covered by else as well
                else -> AnyCol(key, arrayOf(sumValue))
            }.let { sumCols.add(it) }
        }

        return SimpleDataFrame(sumCols)
    }


    //    https://kotlinlang.org/docs/reference/multi-declarations.html
    //    operator fun component1() = 1

    // todo enforce better typed API
    override fun addColumn(tf: ColumnFormula): DataFrame {

        val mutation = tf.expression(this.ec, this.ec)
        val newCol = anyAsColumn(mutation, tf.name, nrow)


        require(newCol.values().size == nrow) { "new column has inconsistent length" }
        require(newCol.name != tempColumnName()) { "missing name in new columns" }

        return if (newCol.name in names) replaceColumn(newCol) else addColumn(newCol)
    }

    // todo should this be public API
    private fun SimpleDataFrame.replaceColumn(newCol: DataCol): DataFrame {
        val newColIndex = names.indexOf(newCol.name)
        require(newColIndex >= 0) { "columns $newCol does not exist in data-frame" }

        return cols.toMutableList().apply {
            removeAt(newColIndex)
            add(newColIndex, newCol)
        }.let { SimpleDataFrame(it) }
    }


    override fun sortedBy(vararg by: String): DataFrame {
        if (by.isEmpty()) {
            warning("[krangl] Calling sortedBy without arguments lacks meaningful semantics")
            return this
        }

        // utility method to convert columns to comparators
        fun asComparator(by: String): Comparator<Int> {
            val dataCol = this[by]
            //            return naturalOrder<*>()
            return dataCol.createComparator()
        }

        // https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.comparisons/java.util.-comparator/then-by-descending.html
        // https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.comparisons/index.html
        val compChain = by.map { asComparator(it) }.reduce { a, b -> a.then(b) }


        // see http://stackoverflow.com/questions/11997326/how-to-find-the-permutation-of-a-sort-in-java
        val permutation = (0..(nrow - 1)).sortedWith(compChain).toIntArray()

        // apply permutation to all columns
        return cols.map {
            when (it) {
                is DoubleCol -> DoubleCol(it.name, Array(nrow, { index -> it.values[permutation[index]] }))
                is IntCol -> IntCol(it.name, Array(nrow, { index -> it.values[permutation[index]] }))
                is LongCol -> LongCol(it.name, Array(nrow, { index -> it.values[permutation[index]] }))
                is BooleanCol -> BooleanCol(it.name, Array(nrow, { index -> it.values[permutation[index]] }))
                is StringCol -> StringCol(it.name, Array(nrow, { index -> it.values[permutation[index]] }))
                is AnyCol -> AnyCol(it.name, Array(nrow, { index -> it.values[permutation[index]] }))
                else -> throw UnsupportedOperationException()
            }
        }.let {
            SimpleDataFrame(it)
        }
    }


    // use proper generics here
    override fun groupBy(vararg by: String): DataFrame {
        if (by.isEmpty()) System.err.print("Grouping with empty attribute list is unlikely to have meaningful semantics")

        // todo test if data is already grouped by the given 'by' and skip regrouping if so

        //take all grouping columns
        val groupCols = select(*by) //cols.filter { by.contains(it.name) }
        require(groupCols.ncol == by.size) { "Could not find all grouping columns" }

        val EMPTY_BY_HASH = Random().nextInt()

        // extract the group value-tuple for each row and calculate row-hashes
        // note: when joining with empty `by` row hashes would be 1 which would result in incorrect cartesian products
        //       thus

        // old (SLOW!) named row iterator approach
        //        val rowHashes = rows.map { row ->
        //            if (by.isEmpty()) {
        //                EMPTY_BY_HASH
        //            } else {
        //                by.map { row[it]?.hashCode() ?: NA_GROUP_HASH }.hashCode()
        //            }
        //        }

        // new (FAST!) raw row iterator approach
        val rowHashes: Iterable<GroupKey> = if (by.isEmpty()) {
            IntArray(nrow, { EMPTY_BY_HASH }).map { it -> listOf<Any?>(it) }
        } else {
            groupCols.rowData()
        }

        // and now split up original dataframe columns by selector index
        val groupIndices = rowHashes.mapIndexed { index, group -> Pair(group, index) }.groupBy { it.first }.map {
            val groupRowIndices = it.value.map { it.second }.toIntArray()
            GroupIndex(it.key, groupRowIndices)
        }


        //        Array<String>(3, { it.toString()}).toList()
        //        arrayListOf<String>(3, { it.toString()})

        fun extractGroup(col: DataCol, groupIndex: GroupIndex): DataCol = when (col) {
            // create new array
            is DoubleCol -> DoubleCol(
                col.name,
                Array(groupIndex.rowIndices.size, { col.values[groupIndex.rowIndices[it]] })
            )
            is IntCol -> IntCol(col.name, Array(groupIndex.rowIndices.size, { col.values[groupIndex.rowIndices[it]] }))
            is LongCol -> LongCol(
                col.name,
                Array(groupIndex.rowIndices.size, { col.values[groupIndex.rowIndices[it]] })
            )
            is BooleanCol -> BooleanCol(
                col.name,
                Array(groupIndex.rowIndices.size, { col.values[groupIndex.rowIndices[it]] })
            )
            is StringCol -> StringCol(
                col.name,
                Array(groupIndex.rowIndices.size, { col.values[groupIndex.rowIndices[it]] })
            )
            is AnyCol -> AnyCol(col.name, Array(groupIndex.rowIndices.size, { col.values[groupIndex.rowIndices[it]] }))
            else -> throw UnsupportedOperationException()
        }

        fun extractGroupByIndex(groupIndex: GroupIndex, df: SimpleDataFrame): SimpleDataFrame {
            val grpSubCols = df.cols.map { extractGroup(it, groupIndex) }

            // todo change order so that group columns come first
            return SimpleDataFrame(grpSubCols)
        }


        var groups = groupIndices.map { DataGroup(it.groupHash, extractGroupByIndex(it, this)) }

        // preserve column structure in empty data-frames
        if (groups.isEmpty()) {
            groups = listOf(DataGroup(listOf(1), this))
        }

        return GroupedDataFrame(by.toList(), groups)
    }


    override fun groupedBy(): DataFrame = emptyDataFrame()


    override fun groups(): List<DataFrame> = listOf(this)


    override fun ungroup(): DataFrame = this // for ungrouped data ungrouping won't do anything

    override fun toString(): String = asString()


    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other?.javaClass != javaClass) return false

        other as SimpleDataFrame

        if (cols != other.cols) return false

        return true
    }

    override fun hashCode(): Int {
        return cols.hashCode()
    }


}

internal typealias GroupKey = List<Any?>


internal val DataFrame.ec: ExpressionContext
    get() = ExpressionContext(this)


internal fun anyAsColumn(mutation: Any?, name: String, nrow: Int): DataCol {
    // expand scalar values to arrays/lists
    val arrifiedMutation: Any? = when (mutation) {
        is Int -> IntArray(nrow, { mutation })
        is Double -> DoubleArray(nrow, { mutation })
        is Boolean -> BooleanArray(nrow, { mutation })
        is Float -> FloatArray(nrow, { mutation })
        is String -> Array<String>(nrow) { mutation }
        // add/test NA support here
        else -> mutation
        //        else -> Array<Any?>(nrow) { mutation }
    }

    val newCol = when (arrifiedMutation) {
        is DataCol -> when (arrifiedMutation) {
            is DoubleCol -> DoubleCol(name, arrifiedMutation.values)
            is IntCol -> IntCol(name, arrifiedMutation.values)
            is LongCol -> LongCol(name, arrifiedMutation.values)
            is StringCol -> StringCol(name, arrifiedMutation.values)
            is BooleanCol -> BooleanCol(name, arrifiedMutation.values)
            is AnyCol -> AnyCol(name, arrifiedMutation.values)
            else -> throw UnsupportedOperationException()
        }

        // todo still needed?
        is DoubleArray -> DoubleCol(name, arrifiedMutation.run { Array<Double?>(size, { this[it] }) })
        is IntArray -> IntCol(name, arrifiedMutation.run { Array<Int?>(size, { this[it] }) })
        is LongArray -> LongCol(name, arrifiedMutation.run { Array<Long?>(size, { this[it] }) })
        is BooleanArray -> BooleanCol(name, arrifiedMutation.run { Array<Boolean?>(size, { this[it] }) })

        // also handle lists here
        emptyList<Any>() -> AnyCol(name, emptyArray())
        emptyArray<Any>() -> AnyCol(name, emptyArray())
        is Array<*> -> handleArrayErasure(name, arrifiedMutation)
        is List<*> -> handleListErasure(name, arrifiedMutation)
        is Sequence<*> -> handleListErasure(name, arrifiedMutation.toList())

        else -> AnyCol(name, Array(nrow, { arrifiedMutation }))
    }
    return newCol
}


/**Test if for first non-null elemeent in list if it has specific type bu peeking into it from the top. */
inline fun <reified T> isOfType(items: Array<Any?>): Boolean {
    val it = items.iterator()

    while (it.hasNext()) {
        if (it.next() is T) return true
    }

    return false
}


// wrapped so that we can expose them without polluting the krangl namespace
object ArrayUtils {

    @Suppress("UNCHECKED_CAST")
    internal fun handleArrayErasure(otherCol: DataCol, name: String, mutation: Array<*>): DataCol = when (otherCol) {
        //    isOfType<Int>(mutation as Array<Any?>) -> IntCol(name, mutation as Array<Int?>)
        is IntCol -> IntCol(name, Array<Int?>(mutation.size, { mutation[it] as? Int }))
        is LongCol -> LongCol(name, Array<Long?>(mutation.size, { mutation[it] as? Long }))
        is StringCol -> StringCol(name, Array<String?>(mutation.size, { mutation[it] as? String }))
        is DoubleCol -> DoubleCol(name, Array<Double?>(mutation.size, { mutation[it] as? Double }))
        is BooleanCol -> BooleanCol(name, Array<Boolean?>(mutation.size, { mutation[it] as? Boolean }))
        else -> AnyCol(name, mutation as Array<Any?>)
    }

    @Suppress("UNCHECKED_CAST")
    internal fun handleArrayErasure(name: String, mutation: Array<*>): DataCol = when {
        //    isOfType<Int>(mutation as Array<Any?>) -> IntCol(name, mutation as Array<Int?>)
        isOfType<Int>(mutation as Array<Any?>) -> IntCol(name, Array<Int?>(mutation.size, { mutation[it] as? Int }))
        isOfType<Long>(mutation as Array<Any?>) -> LongCol(name, Array<Long?>(mutation.size, { mutation[it] as? Long }))
        isOfType<String>(mutation) -> StringCol(name, Array<String?>(mutation.size, { mutation[it] as? String }))
        isOfType<Double>(mutation) -> DoubleCol(name, Array<Double?>(mutation.size, { mutation[it] as? Double }))
        isOfType<Boolean>(mutation) -> BooleanCol(name, Array<Boolean?>(mutation.size, { mutation[it] as? Boolean }))
        isOfType<Any>(mutation) -> AnyCol(name, mutation)
        mutation.isEmpty() -> AnyCol(name, emptyArray())
        else -> throw UnsupportedOperationException()
    }

    @Suppress("UNCHECKED_CAST")
    fun handleListErasure(name: String, mutation: List<*>): DataCol = when {
        isListOfType<Int?>(mutation) -> IntCol(name, mutation as List<Int?>)
        isListOfType<Long?>(mutation) -> LongCol(name, mutation as List<Long?>)
        isListOfType<String?>(mutation) -> StringCol(name, mutation as List<String?>)
        isListOfType<Double?>(mutation) -> DoubleCol(name, mutation as List<Double?>)
        isListOfType<Boolean?>(mutation) -> BooleanCol(name, mutation as List<Boolean?>)
        isMixedNumeric(mutation) -> DoubleCol(name, mutation.map { (it as? Number)?.toDouble() })
        mutation.isEmpty() -> AnyCol(name, emptyArray())
        else -> AnyCol(name, mutation)
        //    else -> throw UnsupportedOperationException()
    }
}

inline fun <reified T> isListOfType(items: List<Any?>): Boolean {
    val it = items.iterator()

    while (it.hasNext()) {
        if (it.next() !is T) return false
    }

    return true
}


private fun isMixedNumeric(mutation: List<*>): Boolean {
    val it = mutation.iterator()

    while (it.hasNext()) {
        val next = it.next()
        if (!(next == null || next is Double || next is Int)) return false
    }

    return true
}

//@Suppress("UNCHECKED_CAST")
//internal fun handleListErasureOLD(name: String, mutation: List<*>): DataCol = when (mutation.first()) {
//    is Double -> DoubleCol(name, mutation as List<Double>)
//    is Int -> IntCol(name, mutation as List<Int>)
//    is Long -> LongCol(name, mutation as List<Long>)
//    is String -> StringCol(name, mutation as List<String>)
//    is Boolean -> BooleanCol(name, mutation as List<Boolean>)
//    else -> throw UnsupportedOperationException()
//}


internal fun <T> arrayListOf(size: Int, initFun: (Int) -> T?): ArrayList<T?> {
    val arrayList = ArrayList<T?>(size)

    for (i in 0..(size - 1)) {
        arrayList.add(i, initFun(i))
    }

    return arrayList
}
