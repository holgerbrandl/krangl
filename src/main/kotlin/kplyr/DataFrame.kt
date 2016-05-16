package kutils.kplyr

import java.util.*
import kotlin.comparisons.then


interface DataFrame {

    /** Mutate adds new variables and preserves existing.*/
    fun mutate(name: String, formula: (DataFrame) -> Any): DataFrame

    fun arrange(vararg by: String): DataFrame


    /** select() keeps only the variables you mention.*/
    fun select(which: List<Boolean>): DataFrame

    fun filter(predicate: (DataFrame) -> BooleanArray): DataFrame

    fun groupBy(vararg by: String): DataFrame

    fun summarize(name: String, formula: (DataFrame) -> Any): DataFrame


    // accessor functions

    /** @return Number of rows in this dataframe. */
    val nrow: Int
    /** @return Number of columns in this dataframe. */
    val ncol: Int

    /** Returns the ordered list of column names of this data-frame. */
    val names: List<String>

    operator fun get(name: String): DataCol

    // todo use invoke() style operator here (see https://kotlinlang.org/docs/reference/operator-overloading.html)
    fun row(rowIndex: Int): Map<String, Any>

}


open class SimpleDataFrame(val cols: List<DataCol>) : DataFrame {
    override fun select(which: List<Boolean>): DataFrame = SimpleDataFrame(cols.filterIndexed { index, dataCol -> which[index] })

    // Utility methods

    override fun row(rowIndex: Int): Map<String, Any> =
            cols.map {
                it.name to when (it) {
                    is DoubleCol -> it.values[rowIndex]
                    is IntCol -> it.values[rowIndex]
                    is StringCol -> it.values[rowIndex]
                    else -> throw UnsupportedOperationException()
                }
            }.toMap()

    override val ncol = cols.size

    override val nrow by lazy {
        val firstCol = cols.first()
        when (firstCol) {
            is DoubleCol -> firstCol.values.size
            is IntCol -> firstCol.values.size
            is StringCol -> firstCol.values.size
            else -> throw UnsupportedOperationException()
        }
    }


    /** This method is private to enforce use of mutate which is the primary way to add columns in kplyr. */
    private fun addColumn(newCol: DataCol): SimpleDataFrame {
        require(newCol.length == nrow) { "Column lengths of dataframe ($nrow) and new column (${newCol.length}) differ" }
        require(newCol.name !in names) { "Column '${newCol.name}' already exists in dataframe" }

        val mutatedCols = cols.toMutableList().apply { add(newCol) }
        return SimpleDataFrame(mutatedCols.toList())
    }

    /** Returns the ordered list of column names of this data-frame. */
    override val names: List<String> = cols.map { it.name }


    override operator fun get(name: String): DataCol {
        return cols.find({ it -> it.name == name })!!
    }

    // Core Verbs

    override fun filter(predicate: (DataFrame) -> BooleanArray): DataFrame {
        val indexFilter = predicate(this)

        return cols.map {
            // subset a colum by the predicate array
            when (it) {
                is DoubleCol -> DoubleCol(it.name, it.values.filterIndexed { index, value -> indexFilter[index] }.toDoubleArray())
                is IntCol -> IntCol(it.name, it.values.filterIndexed { index, value -> indexFilter[index] }.toIntArray())
                is StringCol -> StringCol(it.name, it.values.filterIndexed { index, value -> indexFilter[index] }.toList())
                else -> throw UnsupportedOperationException()
            }
        }.let { SimpleDataFrame(it) }
    }


    // also provide vararg constructor for convenience
    constructor(vararg cols: DataCol) : this(cols.asList())

    override fun summarize(name: String, formula: (DataFrame) -> Any): DataFrame {
        throw UnsupportedOperationException()
    }

//    https://kotlinlang.org/docs/reference/multi-declarations.html
//    operator fun component1() = 1

    // todo somehow make private to enforce better typed API
    override fun mutate(name: String, formula: (DataFrame) -> Any): DataFrame {

        val mutation = formula(this)

        // expand scalar values to arrays/lists
        val arrifiedMutation: Any = when (mutation) {
            is Int -> IntArray(nrow, { mutation })
            is Double -> DoubleArray(nrow, { mutation })
            is Boolean -> BooleanArray(nrow, { mutation })
            is Float -> FloatArray(nrow, { mutation })
            is String -> Array<String>(nrow) { mutation }.asList()
            else -> mutation
        }


        // unwrap existing columns to use immutable one with given name
//        val mutUnwrapped = {}

        val newCol = when (arrifiedMutation) {
//            is DataCol -> mutation.apply { setName(name) }
            is DoubleArray -> DoubleCol(name, arrifiedMutation)
            is BooleanArray -> BooleanCol(name, arrifiedMutation)
        // todo too weakly typed
            is List<*> -> if (arrifiedMutation.first() is String) StringCol(name, arrifiedMutation as List<String>) else throw UnsupportedOperationException()
            else -> throw UnsupportedOperationException()
        }
        return addColumn(newCol)
    }


    override fun arrange(vararg by: String): DataFrame {

        // utility method to convert columns to comparators
        fun asComparator(by: String): Comparator<Int> {
            val dataCol = this[by]
//            return naturalOrder<*>()
            return when (dataCol) {
                is DoubleCol -> Comparator { left, right -> dataCol.values[left].compareTo(dataCol.values[right]) }
                is IntCol -> Comparator { left, right -> dataCol.values[left].compareTo(dataCol.values[right]) }
                is StringCol -> Comparator { left, right -> dataCol.values[left].compareTo(dataCol.values[right]) }
                is BooleanCol -> Comparator { left, right -> dataCol.values[left].compareTo(dataCol.values[right]) }
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
                is DoubleCol -> DoubleCol(it.name, DoubleArray(nrow, { index -> it.values[permutation[index]] }))
                is IntCol -> IntCol(it.name, IntArray(nrow, { index -> it.values[permutation[index]] }))
                is StringCol -> StringCol(it.name, Array(nrow, { index -> it.values[permutation[index]] }).toList())
                is BooleanCol -> BooleanCol(it.name, BooleanArray(nrow, { index -> it.values[permutation[index]] }))
                else -> throw UnsupportedOperationException()
            }
        }.let { SimpleDataFrame(it) }

    }


    // use proper generics here
    override fun groupBy(vararg by: String): DataFrame {
        //take all grouping columns
        val groupCols = cols.filter { by.contains(it.name) }

        // calculate selector index using hashcode // todo use more efficient scheme to avoid hashing of ints
        val rowHashes: IntArray = groupCols.map { it.colHash() }.foldRight(IntArray(nrow).apply { fill(1) }, IntArray::plus)


        // use filter index for each selector-index
        // see https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.collections/filter-indexed.html

        // and  split up orignal dataframe columns by selector index


        val groupIndicies = rowHashes.
                mapIndexed { index, group -> Pair(index, group) }.
                groupBy { it.first }.
                map {
                    val groupRowIndices = it.value.map { it.second }.toIntArray()
                    GroupIndex(GroupKey(it.key, this.row(groupRowIndices.first())), groupRowIndices)
                }


        fun extractGroup(col: DataCol, groupIndex: GroupIndex): DataCol {
            return when (col) {
                is DoubleCol -> DoubleCol(col.name, col.values.filterIndexed { index, d -> groupIndex.rowIndices.contains(index) }.toDoubleArray())
                is IntCol -> IntCol(col.name, col.values.filterIndexed { index, d -> groupIndex.rowIndices.contains(index) }.toIntArray())
                is StringCol -> StringCol(col.name, col.values.filterIndexed { index, d -> groupIndex.rowIndices.contains(index) }.toList())
                else -> throw UnsupportedOperationException()
            }
        }

        fun extractSubframeByIndex(groupIndex: GroupIndex, df: SimpleDataFrame): SimpleDataFrame {
            val grpSubCols = df.cols.map { extractGroup(it, groupIndex) }
            return SimpleDataFrame(grpSubCols)
        }


        return GroupedDataFrame(groupIndicies.map { it to extractSubframeByIndex(it, this) }.toMap())
    }
}


// To illustrate the structure of th API just core verbs are implemented as instance functions. The rest is implement as extension functions.

// enrich group index with actual value tuple
data class GroupKey(val groupHash: Int, val groupTuple: Map<String, Any> = emptyMap()) // we could use distinct here on the key attributes

data class GroupIndex(val group: GroupKey, val rowIndices: IntArray)


internal class GroupedDataFrame(private val groups: Map<GroupIndex, DataFrame>) : DataFrame {

    override val names: List<String>
        get() = throw UnsupportedOperationException()

    override fun select(which: List<Boolean>): DataFrame {
        throw UnsupportedOperationException()
    }

    override val ncol: Int
        get() = throw UnsupportedOperationException()

    override fun filter(predicate: (DataFrame) -> BooleanArray): DataFrame {
        throw UnsupportedOperationException()
    }

    override fun mutate(name: String, formula: (DataFrame) -> Any): DataFrame {
        throw UnsupportedOperationException()
    }
//    override fun mutate(name: String, formula: (DataFrame) -> Any?): DataFrame {
//        throw UnsupportedOperationException()
//    }

    override fun row(rowIndex: Int): Map<String, Any> {
        // find the group
//        groups.filterKeys { it.rowIndices.contains(rowIndex) }.values.
        throw UnsupportedOperationException()
    }

    override val nrow: Int
        get() = throw UnsupportedOperationException()

    override fun summarize(name: String, formula: (DataFrame) -> Any): DataFrame {
        throw UnsupportedOperationException()
    }


    override fun arrange(vararg by: String): DataFrame {
        throw UnsupportedOperationException()
    }

    override fun groupBy(vararg by: String): DataFrame {

        throw UnsupportedOperationException()
    }

    override fun get(name: String): DataCol {
        // kplyr/CoreVerbsTest.kt:7
        throw UnsupportedOperationException()
    }
}

// Extension function that mimic othe major elements of the dplyr API

fun DataFrame.head(numRows: Int = 5) = filter { IntCol("dummy", rowNumber()) lt numRows }
fun DataFrame.tail(numRows: Int = 5) = filter { IntCol("dummy", rowNumber()) gt (nrow - 5) }
fun DataFrame.rowNumber() = (1..nrow).asSequence().toList().toIntArray()


////////////////////////////////////////////////
// select API
////////////////////////////////////////////////

class ColNames(val names: List<String>)


fun ColNames.matches(regex: String): List<Boolean> = names.map { it.matches(regex.toRegex()) }
fun ColNames.startsWith(prefix: String): List<Boolean> = names.map { it.startsWith(prefix) }
fun ColNames.ends(prefix: String): List<Boolean> = names.map { it.startsWith(prefix) }
fun ColNames.all(vararg colNames: String): List<Boolean> = Array(names.size, { true }).toList()


// since this affects String namespace it might be not a good idea
operator fun String.unaryMinus() = fun ColNames.(): List<Boolean> = names.map { it != this@unaryMinus }

//val test = "dsf"
//val another = -"dsf"

/** Keeps only the variables you mention.*/
fun DataFrame.select(vararg columns: String): DataFrame = select(this.names.map { colName -> columns.contains(colName) })

/** Keeps only the variables that match any of the given expressions. E.g. use `startsWith("foo")` to select for columns staring with 'foo'.*/
fun DataFrame.select(vararg which: ColNames.() -> List<Boolean>): DataFrame {
    return which.fold(Array(nrow, { true }).toBooleanArray(), {
        inital, next ->
        inital AND next(ColNames(names)).toBooleanArray()
    }).let { this.select(it.toList()) }
}


// mutate convencience

/** Mutate adds new variable and drops unused existing variables. */
fun DataFrame.transmute(formula: (DataFrame) -> Any): DataFrame = throw UnsupportedOperationException()

// Select Utilities
// todo make internal and just expose helper functions


// todo implement rename() extension function


// add more formatting options here
fun DataFrame.print(colNames: Boolean = true, sep: String = "\t") {
    // todo add support for grouped data here

    if (this !is SimpleDataFrame) {
        return
    }

    if (colNames) this.cols.map { it.name }.joinToString(sep).apply { println(this) }

    rowNumber().map { row(it - 1).values.joinToString(sep).apply { println(this) } }
}

// add more formatting options here
fun DataFrame.glimpse(sep: String = "\t") {
    // todo add support for grouped data here
    if (this !is SimpleDataFrame) {
        return
    }

    val topN = head(8) as SimpleDataFrame

    for (col in topN.cols) {
        val examples = when (col) {
            is DoubleCol -> col.values.toList()
            is IntCol -> col.values.toList()
            is StringCol -> col.values
            else -> throw UnsupportedOperationException()
        }.joinToString(", ", prefix = col.name + "\t:").apply { println(this) }
        println(col.name + "\t: " + examples)
    }
}
