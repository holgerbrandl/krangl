@file:JvmName("Extensions")

package krangl

import krangl.ArrayUtils.handleArrayErasure
import krangl.util.createComparator
import krangl.util.createValidIdentifier
import krangl.util.joinToMaxLengthString
import java.util.*


////////////////////////////////////////////////
// select() helpers and API
////////////////////////////////////////////////


//
// Data Reshaping  
//

data class RenameRule(val oldName: String, val newName: String) {
    fun asTableFormula() = ColumnFormula(newName, { df -> df[oldName] })
}


// todo dplyr consistency here or "old" to "new" readbility, what's more important (see docs/user_guide.md)
fun DataFrame.rename(vararg old2new: Pair<String, String>) =
    this.rename(*old2new.map { RenameRule(it.first, it.second) }.toTypedArray())


/** Rename one or several columns. Positions should be preserved */
fun DataFrame.rename(vararg old2new: RenameRule): DataFrame {
    // ignore dummy renames like "foo" to "foo" ( can happen when doing unequal joins; also because of consistency)
    val old2NewFilt = old2new.filter { it.oldName != it.newName }

    // create column list with new names at old positions
    val namesRestoredPos = old2NewFilt.fold(names, { adjNames, renRule ->
        adjNames.map { if (it == renRule.oldName) renRule.newName else it }
    })

    // make sure that renaming rule does not contain duplicates to allow for better error reporting
    val renamed = old2NewFilt.fold(this, { df, renRule -> df.addColumn(renRule.asTableFormula()).remove(renRule.oldName) })


    // restore positions of renamed columns
    return renamed.select(*namesRestoredPos.toTypedArray())
}


//
// mutate() convenience
//

/** A proxy on the `df` that exposes just parts of the DataFrame api that are relevant for table expressions
 * @param df A [krangl.DataFrame] instance
 */
class ExpressionContext(val df: DataFrame) {
    operator fun get(name: String): DataCol = df[name]

    // from slack: in general, yes: use lazy mostly for calculating expensive values that might never be needed.
    val rowNumber: List<Int> get() = (1..df.nrow).toList()
    //     val rowNumber: Iterable<Int>  by lazy { (1..nrow) }

    val nrow = df.nrow

}


// as would also prevent us from overwriting to
//infix fun String.to(that: TableExpression) = Pair<String, DataFrame.(DataFrame) -> Any?>(this, that)

infix fun String.to(that: TableExpression) = ColumnFormula(this, that)
// or using backticked =
infix fun String.`=`(that: TableExpression) = ColumnFormula(this, that)
// should we ditch one?
//* to familiar to kotlin users
//* `=` familar to r and python users

// looks odd summarize ("max_age"{ it["age"].max() })
//operator fun String.invoke(that: TableExpression) = ColumnFormula(this, that)


data class ColumnFormula(val name: String, val expression: TableExpression)


//data class ColumnFunction(val name: String, val expression: (DataCol) -> ColumnData)
//
///** Wrapper to allow for more vectorized operations. */
//class ColumnData(val List<Any?>)
//
//fun DataFrame.mutateAt(columns: ColumnSelector, // no default here to avoid signature clash = { all() },
// vararg fun : ColumnFunction
//) : DataFrame {
//    val columns = colSelectAsNames(reduceColSelectors(arrayOf(columns)))
//
//    columns.fold(this, { df, colName -> df.addColumn(colName+){ it[""]}
//
//    }
//}


////////////////////////////////////////////////
// filter() convenience
////////////////////////////////////////////////

/** Filter the rows of a table with a single predicate.*/

fun DataFrame.filter(predicate: DataFrame.(DataFrame) -> List<Boolean>): DataFrame = filter({ predicate(this.df).toBooleanArray() })

/** AND-filter a table with different filters.*/
fun DataFrame.filter(vararg predicates: DataFrame.(DataFrame) -> List<Boolean>): DataFrame =
    predicates.fold(this, { df, p -> df.filter(p) })

// // todo does not work why?
// df.filter({ it["last_name"].asStrings().map { it!!.startsWith("Do") } })


/** Match a text column in a NA-aware manner to create a predicate vector for filtering.
 *
 * @sample krangl.samples.textMatching
 */
@Suppress("UNCHECKED_CAST")
fun DataCol.isMatching(missingAs: Boolean = false, filter: String.() -> Boolean): BooleanArray =
    (map<String> { it.filter() }.map { it ?: missingAs } as List<Boolean>).toBooleanArray()


/**
 * Select random rows from a table.  If receiver is grouped, sampling is done per group.
 *
 * @param fraction Fraction of rows to sample.
 * @param replace Sample with or without replacement
 */
fun DataFrame.sampleFrac(fraction: Double, replace: Boolean = false): DataFrame = if (this is GroupedDataFrame) {
    transformGroups({ it.sampleFrac(fraction, replace) })
} else {
    sampleN(Math.round(fraction.toDouble() * nrow).toInt(), replace)
}


/**
 * Select random rows from a table. If receiver is grouped, sampling is done per group.
 *
 * @param n Number of rows to sample.
 * @param replace Sample with or without replacement
 */
fun DataFrame.sampleN(n: Int, replace: Boolean = false): DataFrame {
    if (this is GroupedDataFrame) {
        return transformGroups { it.sampleN(n, replace) }
    }

    require(replace || n <= nrow) { "can not over-sample data without replacement (nrow<${n})" }
    require(n >= 0) { "Sample size must be greater equal than 0 but was ${n}" }

    // depending on replacement-mode randomly sample the index vector
    val sampling: List<Int> = if (replace) {
        mutableListOf<Int>().apply { while (size < n) add(_rand.nextInt(nrow)) }

    } else {
        // from http://stackoverflow.com/questions/196017/unique-non-repeating-random-numbers-in-o1#196065

        // 1. Create a list, 0..1000. + 2. Shuffle the list.
        val shufIndices = (0..(nrow - 1)).toMutableList().apply { Collections.shuffle(this, _rand) }

        // 3. Return numbers in order from the shuffled list.
        shufIndices.subList(0, n)
    }

    // // more pretty but does not allow to do over-sampling
    // //build the filter and subset the data
    // return filter({ BooleanArray(nrow, { sampling.contains(it) }) })

    return this.cols.map { column ->
        handleArrayErasure(column, column.name, Array(sampling.size, { column.values()[sampling[it]] }))
    }.let { SimpleDataFrame(it) }
}

/** Randomize the row order of a data-frame. */
fun DataFrame.shuffle(): DataFrame = sampleN(nrow)


/** Random number generator used to row sampling. Reassignable to set seed for deterministic sampling. */
var _rand = Random(3)


////////////////////////////////////////////////
// sortedBy() convenience
////////////////////////////////////////////////

// allow to rather use selectors
//TODO report incorrect highlighting to JB

//fun DataFrame.sortedBy(tableExpression: TableExpression): DataFrame = sortedBy(*arrayOf(tableExpression))

fun DataFrame.sortedBy(tableExpression: TableExpression) = sortedBy(*arrayOf(tableExpression))

fun DataFrame.sortedBy(vararg tableExpressions: TableExpression): DataFrame {
    // create derived data frame sort by new columns trash new columns
    val sortBys = tableExpressions.mapIndexed { index, value -> "__sort$index" to value }
    val sortByNames = sortBys.map { it.name }.toTypedArray()

    return addColumns(*sortBys.toTypedArray()).sortedBy(*sortByNames).remove(sortByNames.asList())
    //           select({ listOf(*sortByNames).not() })
}


// todo we may want to introduce a subtype of ExpressionContext for sorting. Currently it will be visible in all contexts
fun ExpressionContext.desc(dataCol: DataCol) = rank(dataCol).reversed().let { IntCol(UUID.randomUUID().toString(), it) }


fun ExpressionContext.rank(dataCol: DataCol): List<Int> {
    val comparator = dataCol.createComparator()

    // see http://stackoverflow.com/questions/11997326/how-to-find-the-permutation-of-a-sort-in-java
    val permutation = (0..(nrow - 1)).sortedWith(comparator)

    return permutation
}


////////////////////////////////////////////////
// summarize() convenience
////////////////////////////////////////////////


fun DataFrame.summarize(name: String, tableExpression: TableExpression): DataFrame = summarize(name to tableExpression)


//fun DataFrame.count(name: String, expression: TableExpression): DataFrame = summarize(name to expression)

/** Retain only unique/distinct rows from an input tbl.
 *
 * @arg selects: Variables to use when determining uniqueness. If there are multiple rows for a given combination of inputs, only the first row will be preserved.
 */
// todo provide more efficient implementation
fun DataFrame.distinct(vararg selects: String = this.names.toTypedArray()): DataFrame =
    groupBy(*selects).slice(1).ungroup()


/**
 * Counts observations by group.
 *
 * If no grouping attributes are provided the method will respect the grouping of the receiver, or in cases of an
 * ungrouped receiver will simply count the rows in the data.frame
 *
 * @param selects The variables to to be used for cross-tabulation.
 * @param name The name of the count column resulting table.
 */
fun DataFrame.count(vararg selects: String, name: String = "n"): DataFrame = when {
    selects.isNotEmpty() -> select(*selects).groupBy(*selects).summarize(name, { nrow })
    this is GroupedDataFrame -> select(*selects).summarize(name, { nrow })
    else -> dataFrameOf(name)(nrow)
}


////////////////////////////////////////////////
// General Utilities
////////////////////////////////////////////////


// Extension function that mimic other major elements of the dplyr API

//fun DataFrame.rowNumber() = IntCol(TMP_COLUMN, (1..nrow).asSequence().toList())
//fun DataFrame.rowNumber() = IntCol("row_number", (1..nrow).toList() )

fun DataFrame.take(numRows: Int = 5) = filter {
    rowNumber.map { it <= numRows }.toBooleanArray()
}

fun DataFrame.takeLast(numRows: Int) = filter { rowNumber.map { it > (nrow - numRows) }.toBooleanArray() }

// r-like convenience wrappers
fun DataFrame.head(numRows: Int = 5) = take(numRows)

fun DataFrame.tail(numRows: Int = 5) = takeLast(numRows)


/** Creates a grouped data-frame where each group consists of exactly one line. Thereby the row-number is used a group-hash. */
fun DataFrame.rowwise(): DataFrame {

    val rowsAsGroups: List<DataGroup> = (1..nrow).map { rowIndex ->
        DataGroup(rowIndex, filter { BooleanArray(nrow, { index -> index == rowIndex }) })
    }.toList()

    return GroupedDataFrame(by = listOf("_row_"), groups = rowsAsGroups)
}


/* Select rows by position.
 * Similar to dplyr::slice this operation works in a grouped manner.
 */
fun DataFrame.slice(vararg slices: Int) = filter { rowNumber.map { slices.contains(it) }.toBooleanArray() }

// note: supporting n() here seems pointless since nrow will also work in them mutate context


/* Prints a dataframe to stdout. df.toString() will also work but has no options .*/
@JvmOverloads
fun DataFrame.print(colNames: Boolean = true, maxRows: Int = 20) = println(asString(colNames, maxRows) + "\n")


fun DataFrame.asString(colNames: Boolean = true, maxRows: Int = 20, maxDigits: Int = 3): String {

    var df = this

    if (this !is SimpleDataFrame) {
        df = this.ungroup() as SimpleDataFrame
    }

    // needed for smartcase
    if (df !is SimpleDataFrame) {
        throw UnsupportedOperationException()
    }

    val printData = take(Math.min(nrow, maxRows))

    val valuePrinter = createValuePrinter(maxDigits)

    // calculate indents
    val colWidths = printData.cols.map { it.values().map { (valuePrinter(it)).length }.max() ?: 20 }
    val headerWidths = printData.names.map { it.length }

    // todo mimic dplyr.print better here (num observations, hide too many columns, etc.)

    // detect column padding
    val columnSpacing = 3
    val padding = colWidths.zip(headerWidths)
        .map { (col, head) -> listOf(col, head).max()!! + columnSpacing }
        // remove spacer from first column to have correction alignment with beginning of line
        .toMutableList().also { if (it.size > 0) it[0] -= columnSpacing }.toList()

    val sb = StringBuilder()

    sb.appendln("A DataFrame: ${nrow} x ${ncol}")

    if (this is GroupedDataFrame) {
        sb.append("Groups: ${by.joinToString()} ${groups.size}")
    }


    if (colNames) df.cols.mapIndexed { index, col ->
        col.name.padStart(padding[index])
    }.joinToString("").apply {
        sb.appendln(this)
    }

    printData.rows.map { it.values }.map { rowData ->
        // show null as NA when printing data
        rowData.mapIndexed { index, value ->
            valuePrinter(value).padStart(padding[index])
        }.joinToString("").apply { sb.appendln(this) }
    }

    return sb.trim().toString()
}


data class ColSpec(val pos: Int, val name: String, val type: String)


internal fun getColType(col: DataCol) = when (col) {
    is AnyCol -> col.values.first()?.javaClass?.simpleName
    else -> col.javaClass.simpleName.replace("Col", "")
}


//todo should this be part of the public api? It's not needed in most cases
fun DataFrame.columnTypes(): List<ColSpec> {
    // todo add support for grouped data here
    if (this !is SimpleDataFrame) {
        TODO()
    }

    val foo = mapOf("index" to 1, "name" to "foo")

    return cols.mapIndexed { index, col -> ColSpec(index, col.name, getColType(col) ?: "") }
}

fun List<ColSpec>.asDf() = deparseRecords { mapOf("index" to it.pos, "name" to it.name, "type" to it.type) }

fun List<ColSpec>.print() = asDf().print()


// todo should it be called structure of glimpse. there should not be any name aliases
@Deprecated("use schema instead", replaceWith = ReplaceWith("schema()"))
fun DataFrame.glimpse() = schema()


// see https://spark.apache.org/docs/latest/sql-programming-guide.html#untyped-dataset-operations-aka-dataframe-operations
/**
 *  Prints the schema (that is column names, types, and the first few values per column) of a dataframe to stdout.
 */
fun DataFrame.schema(maxDigits: Int = 3, maxLength: Int = 80) {
    if (this is GroupedDataFrame) {
        ungroup().schema(maxDigits, maxLength)
        return
    }

    val topN = this
    println("DataFrame with ${nrow} observations")

    val namePadding = topN.cols.map { it.name.length }.max() ?: 0

    val typeLabels = topN.cols.map { col ->
        when (col) {
            is DoubleCol -> "[Dbl]"
            is IntCol -> "[Int]"
            is StringCol -> "[Str]"
            is BooleanCol -> "[Bol]"
            is AnyCol -> "[${getTypeName(col)}]"
            else -> throw UnsupportedOperationException()
        }
    }

    val typePadding = typeLabels.map { it.length }.max() ?: 0

    topN.cols.zip(typeLabels).forEach { (col, typeLabel) ->
        val stringifiedVals = col.values().asSequence()
            .joinToMaxLengthString(maxLength = maxLength, transform = createValuePrinter(maxDigits))

        println("${col.name.padEnd(namePadding)}  ${typeLabel.padEnd(typePadding)}  $stringifiedVals")
    }
}

private fun getTypeName(col: AnyCol): String {
    val firstEl = col.values.asSequence().filterNotNull().firstOrNull()

    if (firstEl == null) return "Any"

    return firstEl.javaClass.simpleName
        // tweak types for nested data
        .replace("SimpleDataFrame", "DataFrame")
        .replace("GroupedDataFrame", "DataFrame")
}

internal fun createValuePrinter(maxDigits: Int = 3): (Any?) -> String = {
    it?.let { value ->
        when (value) {
            is Double -> value.format(maxDigits)
            is DataFrame -> "<DataFrame [${value.nrow} x ${value.ncol}]>" // see iris %>% group_by(Species) %>% nest
            else -> value.toString()
        }
    } ?: "<NA>"
}

/** Provides a code to convert  a dataframe to a strongly typed list of kotlin data-class instances.*/
fun DataFrame.printDataClassSchema(
    dataClassName: String,
    receiverVarName: String = "dataFrame"
) {
    val df = this.ungroup() as SimpleDataFrame

    // create type
    booleanArrayOf(true, false, false).any()
    fun getNullableFlag(it: DataCol) = if (it.isNA().contains(true)) "?" else ""
    val dataSpec = df.cols.map { """val ${createValidIdentifier(it.name)}: ${getScalarColType(it)}${getNullableFlag(it)}""" }.joinToString(", ")
    println("data class ${dataClassName}(${dataSpec})")

    println("val records = ${receiverVarName}.rowsAs<${dataClassName}>()")
}


/** Concatenate a list of data-frame by row. */
fun List<DataFrame>.bindRows(): DataFrame { // add options about NA-fill over non-overlapping columns
    // todo more column model consistency checks here
    // note: use fold to bind with non-overlapping column model

    val bindCols = mutableListOf<DataCol>()

    //    val totalRows = map { it.nrow }.sum()

    for (colName in this.firstOrNull()?.names ?: emptyList()) {
        val colDataCombined: Array<*> = bindColData(colName)

        when (this.first()[colName]) {
            is DoubleCol -> DoubleCol(colName, colDataCombined.map { it as Double? })
            is IntCol -> IntCol(colName, colDataCombined.map { it as Int? })
            is StringCol -> StringCol(colName, colDataCombined.map { it as String? })
            is BooleanCol -> BooleanCol(colName, colDataCombined.map { it as Boolean? })
            is AnyCol -> AnyCol(colName, colDataCombined.toList())
            else -> throw UnsupportedOperationException()
        }.apply { bindCols.add(this) }

    }

    return SimpleDataFrame(bindCols)
}

fun bindCols(left: DataFrame, right: DataFrame): DataFrame { // add options about NA-fill over non-overlapping columns
    return SimpleDataFrame(left.cols.toMutableList().apply { addAll((right as SimpleDataFrame).cols) })
}

private fun List<DataFrame>.bindColData(colName: String): Array<*> {
    val totalRows = map { it.nrow }.sum()

    val arrayList = Array<Any?>(totalRows, { 0 })

    var iter = 0

    forEach {
        it[colName].values().forEach {
            arrayList[iter++] = it
        }
    }

    return arrayList
}

// Misc or TBD


//fun Any?.asCol(df:DataFrame) = anyAsColumn(this, TMP_COLUMN, df.nrow)

// Any.+ overloading does not work and is maybe neight a good idea since it's affecting global operator conventions
//infix operator fun Any.plus(rightCol:DataCol) = anyAsColumn(this, TMP_COLUMN, rightCol.values().size) + rightCol
fun ExpressionContext.const(someThing: Any) = anyAsColumn(someThing, tempColumnName(), nrow)

internal fun warning(msg: String, breakLine: Boolean = true) = if (breakLine) System.err.println(msg) else System.err.print(msg)

internal inline fun warnIf(value: Boolean, lazyMessage: () -> Any): Unit {
    if (value) System.err.println(lazyMessage())
}

internal fun GroupedDataFrame.transformGroups(trafo: (DataFrame) -> DataFrame): GroupedDataFrame =
    groups.map { DataGroup(it.groupHash, trafo(it.df)) }.let { GroupedDataFrame(by, it) }


fun emptyDataFrame(): DataFrame = SimpleDataFrame()


/** Return an iterator over the rows in data in the receiver. */
internal fun DataFrame.rowData(): Iterable<List<Any?>> = when (this) {

    is GroupedDataFrame -> throw UnsupportedOperationException()
    is SimpleDataFrame -> object : Iterable<List<Any?>> {

        override fun iterator() = object : Iterator<List<Any?>> {

            val colIterators = cols.map { it.values().iterator() }.toList()

            override fun hasNext(): Boolean = colIterators.firstOrNull()?.hasNext() ?: false

            override fun next(): List<Any?> = colIterators.map { it.next() }
        }
    }

    else -> throw IllegalArgumentException()
}


internal val DataFrame.rowNumber: List<Int> get() = (1..nrow).toList()
