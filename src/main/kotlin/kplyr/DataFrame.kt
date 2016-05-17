@file:Suppress("unused")

package kplyr

import java.util.*
import kotlin.comparisons.nullsLast
import kotlin.comparisons.then


// as would also prevent us from overwriting to
infix fun String.to(that: DataFrame.(DataFrame) -> Any?) = Pair<String, DataFrame.(DataFrame) -> Any?>(this, that)


interface DataFrame {

    // Core Manipulation Verbs

    /** Mutate adds new variables and preserves existing.*/
    fun mutate(name: String, formula: DataFrame.(DataFrame) -> Any?): DataFrame
    // todo maybe as would be more readible: df.mutate({ mean(it["foo")} as "mean_foo")
    // todo Also support varargs similar to summarize: var newDf = df.mutate({"new_attr" to  ( it["test"] + it["test"] )})


    // todo also support mini-lang in arrange(); eg: df.arrange(desc("foo"))
    fun arrange(vararg by: String): DataFrame


    /** select() keeps only the variables you mention.*/
    fun select(which: List<Boolean>): DataFrame

    // todo consider to use List<Boolean> in signature. We can not provide both because of type erasure
    fun filter(predicate: DataFrame.(DataFrame) -> BooleanArray): DataFrame

    fun groupBy(vararg by: String): DataFrame

    fun summarize(vararg sumRules: Pair<String, DataFrame.(DataFrame) -> Any?>): DataFrame


    // Accessor functions

    /** @return Number of rows in this dataframe. */
    val nrow: Int
    /** @return Number of columns in this dataframe. */
    val ncol: Int

    /** Returns the ordered list of column names of this data-frame. */
    val names: List<String>

    operator fun get(name: String): DataCol


    // todo use invoke() style operator here (see https://kotlinlang.org/docs/reference/operator-overloading.html)
    fun row(rowIndex: Int): Map<String, Any?>

    val rows: Iterable<Map<String, Any?>>

    fun ungroup(): DataFrame
}


open class SimpleDataFrame(val cols: List<DataCol>) : DataFrame {

    override fun ungroup(): DataFrame {
        throw UnsupportedOperationException()
    }

    override val rows = object : Iterable<Map<String, Any?>> {
        override fun iterator() = object : Iterator<Map<String, Any?>> {
            var curRow = 0

            override fun hasNext(): Boolean = curRow < nrow

            override fun next(): Map<String, Any?> = row(curRow++)
        }

    }

    override fun select(which: List<Boolean>): DataFrame = SimpleDataFrame(cols.filterIndexed { index, dataCol -> which[index] })

    // Utility methods

    override fun row(rowIndex: Int): Map<String, Any?> =
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


    override operator fun get(name: String): DataCol = try {
        cols.first { it.name == name }
    } catch(e: NoSuchElementException) {
        throw NoSuchElementException("Could not find column '${name}' in dataframe")
    }

    // Core Verbs

    override fun filter(predicate: DataFrame.(DataFrame) -> BooleanArray): DataFrame {
        val indexFilter = predicate(this)

        require(indexFilter.size == nrow) { "filter index has incompatible length" }

        return cols.map {
            // subset a colum by the predicate array
            when (it) {
                is DoubleCol -> DoubleCol(it.name, it.values.filterIndexed { index, value -> indexFilter[index] })
                is IntCol -> IntCol(it.name, it.values.filterIndexed { index, value -> indexFilter[index] })
                is StringCol -> StringCol(it.name, it.values.filterIndexed { index, value -> indexFilter[index] }.toList())
                else -> throw UnsupportedOperationException()
            }
        }.let { SimpleDataFrame(it) }
    }


    // also provide vararg constructor for convenience
    constructor(vararg cols: DataCol) : this(cols.asList())

    override fun summarize(vararg sumRules: Pair<String, DataFrame.(DataFrame) -> Any?>): DataFrame {
        require(nrow > 0) { "Can not summarize empty data-frame" } // todocan dplyr?

        val sumCols = mutableListOf<DataCol>()
        for ((key, sumRule) in sumRules) {
            val sumValue = sumRule(this)
            when (sumValue) {
                is Int -> IntCol(key, listOf(sumValue))
                is Double -> DoubleCol(key, listOf(sumValue))
                is Boolean -> BooleanCol(key, listOf(sumValue))
                is String -> StringCol(key, Array(1, { sumValue.toString() }).toList())
                else -> throw UnsupportedOperationException()
            }.let { sumCols.add(it) }
        }

        return SimpleDataFrame(sumCols)
    }


//    https://kotlinlang.org/docs/reference/multi-declarations.html
//    operator fun component1() = 1

    // todo enforce better typed API
    override fun mutate(name: String, formula: DataFrame.(DataFrame) -> Any?): DataFrame {

        val mutation = formula(this)

        // expand scalar values to arrays/lists
        val arrifiedMutation: Any? = when (mutation) {
            is Int -> IntArray(nrow, { mutation })
            is Double -> DoubleArray(nrow, { mutation }).toList()
            is Boolean -> BooleanArray(nrow, { mutation })
            is Float -> FloatArray(nrow, { mutation })
            is String -> Array<String>(nrow) { mutation }.asList()
        // add/test NA support here
            else -> mutation
        }

        // unwrap existing columns to use immutable one with given name
//        val mutUnwrapped = {}

        val newCol = when (arrifiedMutation) {
            is DataCol -> when (arrifiedMutation) {
                is DoubleCol -> DoubleCol(name, arrifiedMutation.values)
                is IntCol -> IntCol(name, arrifiedMutation.values)
                is StringCol -> StringCol(name, arrifiedMutation.values)
                is BooleanCol -> BooleanCol(name, arrifiedMutation.values)
                else -> throw UnsupportedOperationException()
            }

        // toodo still needed
            is DoubleArray -> DoubleCol(name, arrifiedMutation.toList())
            is IntArray -> IntCol(name, arrifiedMutation.toList())
            is BooleanArray -> BooleanCol(name, arrifiedMutation.toList())

        // also handle lists here
            is List<*> -> handleListErasure(name, arrifiedMutation)

            else -> throw UnsupportedOperationException()
        }

        require(newCol.values().size == nrow) { "new column has inconsistent length" }
        require(newCol.name != TMP_COLUMN) { "missing name in new columns" }

        return addColumn(newCol)
    }

    @Suppress("UNCHECKED_CAST")
    private fun handleListErasure(name: String, mutation: List<*>): DataCol = when (mutation.first()) {
        is Double -> DoubleCol(name, mutation as List<Double>)
        is Int -> IntCol(name, mutation as List<Int>)
        is String -> StringCol(name, mutation as List<String>)
        is Boolean -> BooleanCol(name, mutation as List<Boolean>)
        else -> throw UnsupportedOperationException()
    }


    override fun arrange(vararg by: String): DataFrame {

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
                is DoubleCol -> DoubleCol(it.name, Array(nrow, { index -> it.values[permutation[index]] }).toList())
                is IntCol -> IntCol(it.name, Array(nrow, { index -> it.values[permutation[index]] }).toList())
                is BooleanCol -> BooleanCol(it.name, Array(nrow, { index -> it.values[permutation[index]] }).toList())
                is StringCol -> StringCol(it.name, Array(nrow, { index -> it.values[permutation[index]] }).toList())
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
//                    @Suppress("UNCHECKED_CAST")
                    GroupIndex(GroupKey(it.key, this.row(groupRowIndices.first()) as Map<String, Any>), groupRowIndices)
                }


        fun extractGroup(col: DataCol, groupIndex: GroupIndex): DataCol {
            return when (col) {
                is DoubleCol -> DoubleCol(col.name, col.values.filterIndexed { index, d -> groupIndex.rowIndices.contains(index) })
                is IntCol -> IntCol(col.name, col.values.filterIndexed { index, d -> groupIndex.rowIndices.contains(index) })
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

    // todo mimic dplyr.print better here (num observations, hide too many columns, etc.)
    override fun toString(): String = head(5).asString()
}


// To illustrate the structure of th API just core verbs are implemented as instance functions. The rest is implement as extension functions.

// enrich group index with actual value tuple
data class GroupKey(val groupHash: Int, val groupTuple: Map<String, Any> = emptyMap()) // we could use distinct here on the key attributes

data class GroupIndex(val group: GroupKey, val rowIndices: IntArray)


internal class GroupedDataFrame(private val groups: Map<GroupIndex, DataFrame>) : DataFrame {
    override fun ungroup(): DataFrame {
        throw UnsupportedOperationException()
    }

    override val rows: Iterable<Map<String, Any>> = throw UnsupportedOperationException()

    override fun summarize(vararg sumRules: Pair<String, DataFrame.(DataFrame) -> Any?>): DataFrame {
        throw UnsupportedOperationException()
    }


    override val names: List<String>
        get() = throw UnsupportedOperationException()

    override fun select(which: List<Boolean>): DataFrame {
        throw UnsupportedOperationException()
    }

    override val ncol: Int
        get() = throw UnsupportedOperationException()

    override fun filter(predicate: DataFrame.(DataFrame) -> BooleanArray): DataFrame {
        throw UnsupportedOperationException()
    }

    override fun mutate(name: String, formula: DataFrame.(DataFrame) -> Any?): DataFrame {
        throw UnsupportedOperationException()
    }
//    override fun mutate(name: String, formula: DataFrame.(DataFrame) -> Any?): DataFrame {
//        throw UnsupportedOperationException()
//    }

    override fun row(rowIndex: Int): Map<String, Any> {
        // find the group
//        groups.filterKeys { it.rowIndices.contains(rowIndex) }.values.
        throw UnsupportedOperationException()
    }

    override val nrow: Int
        get() = throw UnsupportedOperationException()


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
fun DataFrame.tail(numRows: Int = 5) = filter { IntCol("dummy", rowNumber()) gt (nrow - numRows) }
fun DataFrame.rowNumber() = (1..nrow).asSequence().toList()
// supporting n() here seems pointless since nrow will also work in them mutate context


////////////////////////////////////////////////
// select API
////////////////////////////////////////////////

class ColNames(val names: List<String>)

// select utitlies see http://www.rdocumentation.org/packages/dplyr/functions/select
fun ColNames.matches(regex: String) = names.map { it.matches(regex.toRegex()) }

fun ColNames.startsWith(prefix: String) = names.map { it.startsWith(prefix) }
fun ColNames.endsWith(prefix: String) = names.map { it.startsWith(prefix) }
fun ColNames.everything() = Array(names.size, { true }).toList()
fun ColNames.oneOf(someNames: List<String>) = Array(names.size, { someNames.contains(names[it]) }).toList()


// since this affects String namespace it might be not a good idea
operator fun String.unaryMinus() = fun ColNames.(): List<Boolean> = names.map { it != this@unaryMinus }
//val another = -"dsf"

/** Keeps only the variables you mention.*/
fun DataFrame.select(vararg columns: String): DataFrame = select(this.names.map { colName -> columns.contains(colName) })

/** Keeps only the variables that match any of the given expressions. E.g. use `startsWith("foo")` to select for columns staring with 'foo'.*/
fun DataFrame.select(vararg which: ColNames.() -> List<Boolean>): DataFrame {
    return which.drop(1).fold(which.first()(ColNames(names)), {
        initial, next ->
        initial OR next(ColNames(names))
    }).let { select(it.toList()) }
}


// filter convenience
// todo implement transmute() extension function
//fun DataFrame.transmute(formula: DataFrame.(DataFrame) -> Any): DataFrame = throw UnsupportedOperationException()


// mutate convenience
fun DataFrame.filter(predicate: DataFrame.(DataFrame) -> List<Boolean>): DataFrame = filter({ predicate(this).toBooleanArray() })
// // todo does not work why?
// df.filter({ it["last_name"].asStrings().map { it!!.startsWith("Do") } })


// summarize convenience
fun DataFrame.summarize(name: String, formula: DataFrame.(DataFrame) -> Any?): DataFrame = summarize(name to formula)


// Select Utilities

// todo implement rename() extension function


// General Utilities

/* Prints a dataframe to stdout. df.toString() will also work but has no options .*/
fun DataFrame.print(colNames: Boolean = true, sep: String = "\t") = println(asString())


fun DataFrame.asString(colNames: Boolean = true, sep: String = "\t"): String {

    if (this !is SimpleDataFrame) {
        // todo add support for grouped data here
        throw UnsupportedOperationException()
    }

    val sb = StringBuilder()

    if (colNames) this.cols.map { it.name }.joinToString(sep).apply { sb.appendln(this) }

    rowNumber().map { row(it - 1).values.joinToString(sep).apply { sb.appendln(this) } }

    return sb.toString()
}

/* Prints the structure of a dataframe to stdout.*/
fun DataFrame.glimpse(sep: String = "\t") {
    // todo add support for grouped data here
    if (this !is SimpleDataFrame) {
        return
    }

    val topN = head(8) as SimpleDataFrame

    for (col in topN.cols) {
        when (col) {
            is DoubleCol -> listOf("[Dbl]\t", col.values.toList())
            is IntCol -> listOf("[Int]\t", col.values.toList())
            is StringCol -> listOf("[Str]\t", col.values)
            else -> throw UnsupportedOperationException()
        }.joinToString(", ", prefix = col.name + "\t: ").apply { println(this) }
    }
}

/** Provides a code to convert  a dataframe to a strongly typed list of kotlin data-class instances.*/
fun DataFrame.toKotlin(dfVarName: String, dataClassName: String = dfVarName.capitalize()) {

    if (this !is SimpleDataFrame) {
        return
    }


    // create type
    val dataSpec = cols.map { "val ${it.name}: ${getScalarColType(it) }" }.joinToString(", ")
    println("data class ${dataClassName}(${dataSpec})")

    // map dataframe to
    // example: val dfEntries = df.rows.map {row ->  Df(row.get("first_name") as String ) }

    val attrMapping = cols.map { """ row["${it.name}"] as ${getScalarColType(it)}""" }.joinToString(", ")

    println("val ${dfVarName}Entries = ${dfVarName}.rows.map { row -> ${dataClassName}(${attrMapping}) }")
}


internal fun getScalarColType(it: DataCol): String = when (it) {
    is DoubleCol -> "Double"
    is IntCol -> "Int"
    is BooleanCol -> "Boolean"
    is StringCol -> "String"
    else -> throw  UnsupportedOperationException()
}

