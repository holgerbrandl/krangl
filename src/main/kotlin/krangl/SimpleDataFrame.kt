package krangl

import java.util.*


internal class SimpleDataFrame(override val cols: List<DataCol>) : DataFrame {

    // potential performance bottleneck when processing many-groups/joins
    init {
        // validate input columns
        cols.map { it.name }.apply {
            require(distinct().size == cols.size) { "Column names are not unique (${this})" }
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
        warning(columns.isNotEmpty()) { "Calling select() will always return an empty data-frame" }

        require(names.containsAll(columns.asList())) { "not all selected columns (${columns.joinToString(", ")})are contained in table" }
        require(columns.distinct().size == columns.size) { "Columns must not be selected more than once" }

        return columns.fold(SimpleDataFrame(), { df, colName -> df.addColumn(this[colName]) })
    }

    // Utility methods

    override fun row(rowIndex: Int): DataFrameRow =
            cols.map {
                it.name to when (it) {
                    is DoubleCol -> it.values[rowIndex]
                    is IntCol -> it.values[rowIndex]
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
        require(newCol.name != TMP_COLUMN) { "Internal temporary column name should not be expose to user" }

        val mutatedCols = cols.toMutableList().apply { add(newCol) }
        return SimpleDataFrame(mutatedCols.toList())
    }

    /** Returns the ordered list of column names of this data-frame. */
    override val names: List<String> = cols.map { it.name }


    override operator fun get(name: String): DataCol = try {
        cols.first { it.name == name }
    } catch (e: NoSuchElementException) {
        throw NoSuchElementException("Could not find column '${name}' in dataframe")
    }

    // Core Verbs

    override fun filter(predicate: VectorizedRowPredicate): DataFrame {
        val indexFilter = predicate(this.ec, this.ec)

        require(indexFilter.size == nrow) { "filter index has incompatible length" }

        return cols.map {
            // subset a colum by the predicate array
            when (it) {
                is DoubleCol -> DoubleCol(it.name, it.values.filterIndexed { index, value -> indexFilter[index] }.toTypedArray())
                is IntCol -> IntCol(it.name, it.values.filterIndexed { index, value -> indexFilter[index] }.toTypedArray())
                is StringCol -> StringCol(it.name, it.values.filterIndexed { index, _ -> indexFilter[index] }.toList().toTypedArray())
                is BooleanCol -> BooleanCol(it.name, it.values.filterIndexed { index, _ -> indexFilter[index] }.toList().toTypedArray())
                is AnyCol -> AnyCol(it.name, it.values.filterIndexed { index, _ -> indexFilter[index] }.toList().toTypedArray())
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
                is Double -> DoubleCol(key, arrayOf(sumValue as Double?))
                is Boolean -> BooleanCol(key, arrayOf(sumValue as Boolean?))
                is String -> StringCol(key, arrayOf(sumValue as String?))

            // prevent non-scalar summaries. See krangl/test/CoreVerbsTest.kt:165
                is DataCol -> throw NonScalarValueException(key to sumRule, sumValue)
                is IntArray, is BooleanArray, is DoubleArray, is FloatArray -> throw NonScalarValueException(key to sumRule, "Array")
                is Iterable<*> -> throw NonScalarValueException(key to sumRule, "List")

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
        require(newCol.name != TMP_COLUMN) { "missing name in new columns" }

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
            System.err.println("Calling arrange without arguments is not sensible")
            return this
        }

        // utility method to convert columns to comparators
        fun asComparator(by: String): Comparator<Int> {
            val dataCol = this[by]
            //            return naturalOrder<*>()
            return when (dataCol) {
            // todo use nullsLast
                is DoubleCol -> Comparator { left, right -> nullsLast<Double>().compare(dataCol.values[left], dataCol.values[right]) }
                is IntCol -> Comparator { left, right -> nullsLast<Int>().compare(dataCol.values[left], dataCol.values[right]) }
                is BooleanCol -> Comparator { left, right -> nullsLast<Boolean>().compare(dataCol.values[left], dataCol.values[right]) }
                is StringCol -> Comparator { left, right -> nullsLast<String>().compare(dataCol.values[left], dataCol.values[right]) }
                is AnyCol -> Comparator { left, right ->
                    nullsLast<Comparable<Any>>().compare(dataCol.values[left] as Comparable<Any>, dataCol.values[right] as Comparable<Any>)
                }
                else -> throw UnsupportedOperationException()
            }
        }

        // https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.comparisons/java.util.-comparator/then-by-descending.html
        // https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.comparisons/index.html
        val compChain = by.map { asComparator(it) }.let { it.drop(1).fold(it.first(), { a, b -> a.then(b) }) }


        // see http://stackoverflow.com/questions/11997326/how-to-find-the-permutation-of-a-sort-in-java
        val permutation = (0..(nrow - 1)).sortedWith(compChain).toIntArray()

        // apply permutation to all columns
        return cols.map {
            when (it) {
                is DoubleCol -> DoubleCol(it.name, Array(nrow, { index -> it.values[permutation[index]] }))
                is IntCol -> IntCol(it.name, Array(nrow, { index -> it.values[permutation[index]] }))
                is BooleanCol -> BooleanCol(it.name, Array(nrow, { index -> it.values[permutation[index]] }))
                is StringCol -> StringCol(it.name, Array(nrow, { index -> it.values[permutation[index]] }))
                is AnyCol -> AnyCol(it.name, Array(nrow, { index -> it.values[permutation[index]] }))
                else -> throw UnsupportedOperationException()
            }
        }.let { SimpleDataFrame(it) }

    }


    // use proper generics here
    override fun groupBy(vararg by: String): DataFrame {
        if (by.isEmpty()) System.err.print("Grouping with empty attribute list is unlikely to have meaningful semantics")

        // todo test if data is already grouped by the given 'by' and skip regrouping if so

        //take all grouping columns
        val groupCols = select(*by) //cols.filter { by.contains(it.name) }
        require(groupCols.ncol == by.size) { "Could not find all grouping columns" }

        val NA_GROUP_HASH = Int.MAX_VALUE - 123
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

        // old (FAST!) raw row iterator approach
        val rowHashes: List<Int> = if (by.isEmpty()) {
            IntArray(nrow, { EMPTY_BY_HASH }).toList()
        } else {
            groupCols.rowData().map { row: List<Any?> ->
                //                by.map { row[it]?.hashCode() ?: NA_GROUP_HASH }.hashCode()
                // we make the assumption here that group columns are as in `by`
                row.map { it?.hashCode() ?: NA_GROUP_HASH }.hashCode()
            }
        }

        // and  split up original dataframe columns by selector index
        val groupIndices = rowHashes.
                mapIndexed { index, group -> Pair(group, index) }.
                groupBy { it.first }.
                map {
                    val groupRowIndices = it.value.map { it.second }.toIntArray()
                    GroupIndex(it.key, groupRowIndices)
                }


        //        Array<String>(3, { it.toString()}).toList()
        //        arrayListOf<String>(3, { it.toString()})

        fun extractGroup(col: DataCol, groupIndex: GroupIndex): DataCol = when (col) {
        // create new array
            is DoubleCol -> DoubleCol(col.name, Array(groupIndex.rowIndices.size, { col.values[groupIndex.rowIndices[it]] }))
            is IntCol -> IntCol(col.name, Array(groupIndex.rowIndices.size, { col.values[groupIndex.rowIndices[it]] }))
            is BooleanCol -> BooleanCol(col.name, Array(groupIndex.rowIndices.size, { col.values[groupIndex.rowIndices[it]] }))
            is StringCol -> StringCol(col.name, Array(groupIndex.rowIndices.size, { col.values[groupIndex.rowIndices[it]] }))
            else -> throw UnsupportedOperationException()
        }

        fun extractGroupByIndex(groupIndex: GroupIndex, df: SimpleDataFrame): SimpleDataFrame {
            val grpSubCols = df.cols.map { extractGroup(it, groupIndex) }

            // todo change order so that group columns come first
            return SimpleDataFrame(grpSubCols)
        }


        return GroupedDataFrame(by.toList(), groupIndices.map { DataGroup(it.groupHash, extractGroupByIndex(it, this)) })
    }


    override fun ungroup(): DataFrame = this // for ungrouped data ungrouping won't do anything

    // todo mimic dplyr.print better here (num observations, hide too many columns, etc.)
    override fun toString(): String = take(5).asString()


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
    }

    val newCol = when (arrifiedMutation) {
        is DataCol -> when (arrifiedMutation) {
            is DoubleCol -> DoubleCol(name, arrifiedMutation.values)
            is IntCol -> IntCol(name, arrifiedMutation.values)
            is StringCol -> StringCol(name, arrifiedMutation.values)
            is BooleanCol -> BooleanCol(name, arrifiedMutation.values)
            else -> throw UnsupportedOperationException()
        }

    // todo still needed?
        is DoubleArray -> DoubleCol(name, arrifiedMutation.run { Array<Double?>(size, { this[it] }) })
        is IntArray -> IntCol(name, arrifiedMutation.run { Array<Int?>(size, { this[it] }) })
        is BooleanArray -> BooleanCol(name, arrifiedMutation.run { Array<Boolean?>(size, { this[it] }) })

    // also handle lists here
        emptyList<Any>() -> AnyCol(name, emptyArray())
        emptyArray<Any>() -> AnyCol(name, emptyArray())
        is Array<*> -> handleArrayErasure(name, arrifiedMutation)
        is List<*> -> handleListErasure(name, arrifiedMutation)

        else -> throw UnsupportedOperationException()
    }
    return newCol
}

@Suppress("UNCHECKED_CAST")
internal fun handleArrayErasure(otherCol: DataCol, name: String, mutation: Array<*>): DataCol = when (otherCol) {
//    isOfType<Int>(mutation as Array<Any?>) -> IntCol(name, mutation as Array<Int?>)
    is IntCol -> IntCol(name, Array<Int?>(mutation.size, { mutation[it] as? Int }))
    is StringCol -> StringCol(name, Array<String?>(mutation.size, { mutation[it] as? String }))
    is DoubleCol -> DoubleCol(name, Array<Double?>(mutation.size, { mutation[it] as? Double }))
    is BooleanCol -> BooleanCol(name, Array<Boolean?>(mutation.size, { mutation[it] as? Boolean }))
    else -> AnyCol(name, mutation as Array<Any?>)
}

@Suppress("UNCHECKED_CAST")
internal fun handleArrayErasure(name: String, mutation: Array<*>): DataCol = when {
//    isOfType<Int>(mutation as Array<Any?>) -> IntCol(name, mutation as Array<Int?>)
    isOfType<Int>(mutation as Array<Any?>) -> IntCol(name, Array<Int?>(mutation.size, { mutation[it] as? Int }))
//    isOfType<String>(mutation) -> StringCol(name, mutation as Array<String?>)
    isOfType<String>(mutation) -> StringCol(name, Array<String?>(mutation.size, { mutation[it] as? String }))
//    isOfType<Double>(mutation) -> DoubleCol(name, mutation as Array<Double?>)
    isOfType<Double>(mutation) -> DoubleCol(name, Array<Double?>(mutation.size, { mutation[it] as? Double }))
//    isOfType<Boolean>(mutation) -> BooleanCol(name, mutation as Array<Boolean?>)
    isOfType<Boolean>(mutation) -> BooleanCol(name, Array<Boolean?>(mutation.size, { mutation[it] as? Boolean }))
    isOfType<Any>(mutation) -> AnyCol(name, mutation)
    mutation.isEmpty() -> AnyCol(name, emptyArray())
    else -> throw UnsupportedOperationException()
}


/**Test if for first non-null elemeent in list if it has specific type bu peeking into it from the top. */
inline fun <reified T> isOfType(items: Array<Any?>): Boolean {
    val it = items.iterator()

    while (it.hasNext()) {
        if (it.next() is T) return true
    }

    return false
}

@Suppress("UNCHECKED_CAST")
internal fun handleListErasure(name: String, mutation: List<*>): DataCol = when {
    isListOfType<Int>(mutation) -> IntCol(name, mutation as List<Int>)
    isListOfType<String>(mutation) -> StringCol(name, mutation as List<String>)
    isListOfType<Double>(mutation) -> DoubleCol(name, mutation as List<Double>)
    isListOfType<Boolean>(mutation) -> BooleanCol(name, mutation as List<Boolean>)
    mutation.isEmpty() -> AnyCol(name, emptyArray())
    else -> AnyCol(name, mutation)
//    else -> throw UnsupportedOperationException()
}


inline fun <reified T> isListOfType(items: List<Any?>): Boolean {
    val it = items.iterator()

    while (it.hasNext()) {
        if (it.next() is T) return true
    }

    return false
}

//@Suppress("UNCHECKED_CAST")
//internal fun handleListErasureOLD(name: String, mutation: List<*>): DataCol = when (mutation.first()) {
//    is Double -> DoubleCol(name, mutation as List<Double>)
//    is Int -> IntCol(name, mutation as List<Int>)
//    is String -> StringCol(name, mutation as List<String>)
//    is Boolean -> BooleanCol(name, mutation as List<Boolean>)
//    else -> throw UnsupportedOperationException()
//}


// todo this is the same as ColumnNames. Should we use just one type here.
data class TableHeader(val header: List<String>) {


    operator fun invoke(vararg tblData: Any?): DataFrame {
        //        if(tblData.first() is Iterable<Any?>) {
        //            tblData = tblData.first() as Iterable<Any?>
        //        }


        // 1) break into columns
        val rawColumns: List<List<Any?>> = tblData.toList()
                .mapIndexed { i, any -> i.mod(header.size) to any }
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

/**
Create a new data frame in place.

Example:
```
val df = dataFrameOf(
"foo", "bar") (
"ll",   2, 3,
"sdfd", 4, 5,
"sdf",  5, 8
)
```
 */
fun dataFrameOf(vararg header: String) = TableHeader(header.toList())

internal fun SimpleDataFrame.addColumn(dataCol: DataCol): SimpleDataFrame =
        SimpleDataFrame(cols.toMutableList().apply { add(dataCol) })


fun <T> arrayListOf(size: Int, initFun: (Int) -> T?): ArrayList<T?> {
    val arrayList = ArrayList<T?>(size)

    for (i in 0..(size - 1)) {
        arrayList.add(i, initFun(i))
    }

    return arrayList
}
