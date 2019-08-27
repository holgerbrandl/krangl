@file:JvmName("Extensions")

package krangl

import krangl.ArrayUtils.handleArrayErasure
import krangl.util.createComparator
import krangl.util.createValidIdentifier
import krangl.util.joinToMaxLengthString
import krangl.util.scanLeft
import java.util.*
import java.util.regex.Pattern


open class TableContext(val df: DataFrame) {

    operator fun get(name: String): DataCol = df[name]


    // from slack: in general, yes: use lazy mostly for calculating expensive values that might never be needed.
    val rowNumber: List<Int> get() = (1..df.nrow).toList()
    //     val rowNumber: Iterable<Int>  by lazy { (1..nrow) }
}


////////////////////////////////////////////////
// select() helpers and API
////////////////////////////////////////////////


//
// Data Reshaping  
//

data class RenameRule(val oldName: String, val newName: String) {
    fun asTableFormula() = ColumnFormula(newName, { df -> df[oldName] })
}


// no dplyr consistency here but improved readbility
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

/**
 *  Replace current column names with new ones. The number of provided names must match the number of columns.
 */
fun DataFrame.setNames(vararg newNames: String): DataFrame =
        rename(*names.zip(newNames).map { (old, new) -> old to new }.toTypedArray())

////////////////////////////////////////////////
// mutate() convenience
////////////////////////////////////////////////


/** A proxy on the `df` that exposes just parts of the DataFrame api that are relevant for table expressions
 * @param df A [krangl.DataFrame] instance
 */
class ExpressionContext(df: DataFrame) : TableContext(df) {

    val nrow = df.nrow

    /**
     * A numpy equivalent to
     * `df['color'] = np.where(df['Set']=='Z', 'green', 'red')`
     * See https://stackoverflow.com/questions/19913659/pandas-conditional-creation-of-a-series-dataframe-column
     *
     * In R the corresoponding pattern would be mutate(df, foo=if_else())
     *<p>
     *
     * @sample krangl.samples.addColumnExamples
     */
    fun where(booleans: BooleanArray, ifTrue: Any, ifFalse: Any): DataCol {

        val mutationTrue = anyAsColumn(ifTrue, tempColumnName(), nrow)
        val mutationFalse = anyAsColumn(ifFalse, tempColumnName(), nrow)

        // https://stackoverflow.com/questions/50078266/zip-3-lists-of-equal-length
        val result = booleans.zip(mutationTrue.values().zip(mutationFalse.values())).map { (first, data) ->
            if (first) data.first else data.second
        }

        return ArrayUtils.handleListErasure(tempColumnName(), result)
    }
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


/** Add the row-number as column to a data-frame. */
fun DataFrame.addRowNumber(name: String = "row_number") = addColumn(name) { rowNumber }.moveLeft(name)

////////////////////////////////////////////////
// filter() convenience
////////////////////////////////////////////////

/** Filter the rows of a table with a single predicate.*/

// fixme not visible because of same jvm signature
// https://medium.com/@quiro91/getting-to-know-kotlins-extension-functions-some-caveats-to-keep-in-mind-d14d734d108b
fun DataFrame.filter(predicate: ExpressionContext.(ExpressionContext) -> List<Boolean?>): DataFrame = filter({ predicate(this.df.ec).requireNoNulls().toBooleanArray() })

/** AND-filter a table with different filters.*/
fun DataFrame.filter(vararg predicates: DataFrame.(DataFrame) -> List<Boolean>): DataFrame =
        predicates.fold(this, { df, p -> df.filter(p) })


/** Match a text column in a NA-aware manner to create a predicate vector for filtering.
 *
 * @sample krangl.samples.textMatching
 */
@Suppress("UNCHECKED_CAST")
inline fun <reified T> DataCol.isMatching(missingAs: Boolean = false, crossinline filter: T.() -> Boolean): BooleanArray =
        (map<T> { it.filter() }.map { it ?: missingAs } as List<Boolean>).toBooleanArray()


/**
 * Select random rows from a table.  If receiver is grouped, sampling is done per group.
 *
 * @param fraction Fraction of rows to sample.
 * @param replace Sample with or without replacement
 */
fun DataFrame.sampleFrac(fraction: Double, replace: Boolean = false): DataFrame = if (this is GroupedDataFrame) {
    transformGroups({ it.sampleFrac(fraction, replace) })
} else {
    sampleN(Math.round(fraction * nrow).toInt(), replace)
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


/** todo add ties arguemnt. */

// from https://stackoverflow.com/questions/12289224/rank-and-order-in-r
/**
 * `rank` returns the order of each element in an ascending list
 *
 *  `order` returns the index each element would have in an ascending list
 */
fun DataCol.order(naLast: Boolean = true): List<Int> {
    val comparator = createComparator(naLast)

    return (0..(values().size - 1)).sortedWith(comparator)
}

fun DataCol.rank(naLast: Boolean = true): List<Int> = order(naLast)
        .mapIndexed { idx, value -> idx to value }
        .sortedBy { it.second }.map { it.first }


/** A proxy on the `df` that exposes just parts of the DataFrame api that are relevant for sorting
 *
 * @param df A [krangl.DataFrame] instance
 */
class SortingContext(df: DataFrame) : TableContext(df) {

    /** Creates a sorting attribute that inverts the order of the argument */
    fun desc(dataCol: DataCol) = dataCol.desc()

    /** Creates a sorting attribute that inverts the order of the argument */
    fun desc(columnName: String) = desc(this[columnName])
}

/** Creates a sorting attribute that inverts the order of the argument */
fun DataCol.desc() = rank(false).map { -it }.let { IntCol(tempColumnName(), it) }


typealias SortExpression = SortingContext.(SortingContext) -> Any?


//fun DataFrame.sortedBy(tableExpression: TableExpression): DataFrame = sortedBy(*arrayOf(tableExpression))

fun DataFrame.sortedBy(sortExpression: SortExpression) = sortedBy(*arrayOf(sortExpression))

// todo can we enable those and use DataCol as result for SortExpression?
//@JvmName("sortByList")
//fun DataFrame.sortedBy(sortExpression: SortingContext.(SortingContext) -> List<Any?>) = sortedBy{ this}
//@JvmName("sortByArray")
//fun DataFrame.sortedBy(sortExpression: SortingContext.(SortingContext) -> Array<Any?>) = sortedBy(*arrayOf(sortExpression))


fun DataFrame.sortedBy(vararg sortExpressions: SortExpression): DataFrame {
    // create derived data frame sort by new columns trash new columns
    val sortBys = sortExpressions.mapIndexed { index, sortExpr ->
        ColumnFormula("__sort$index") {
            with(SortingContext(it.df)) { sortExpr(this, this) }
                    // prevent scalar string attributes here which are most likely column names.
                    .also { if (it is String) throw InvalidSortingPredicateException(it) }
        }
    }

    val sortByNames = sortBys.map { it.name }.toTypedArray()

    return addColumns(*sortBys.toTypedArray()).sortedBy(*sortByNames).remove(sortByNames.asList())
    //           select({ listOf(*sortByNames).not() })
}


// tbd should this be part of API or rarely used
//fun DataFrame.reversed() = sortedBy { rowNumber.reversed() }

////////////////////////////////////////////////
// summarize() convenience
////////////////////////////////////////////////


fun DataFrame.summarize(name: String, tableExpression: TableExpression): DataFrame = summarize(name to tableExpression)


//fun DataFrame.count(name: String, expression: TableExpression): DataFrame = summarize(name to expression)

/** Retain only unique/distinct rows from an input tbl.
 *
 * @arg selects: Variables to use when determining uniqueness. If there are multiple rows for a given combination of inputs, only the first row will be preserved.
 */
// a more efficient impl should simply hash the `selects` and transform it in to a predicate function
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


/**
 * Counts expressions
 *
 * If no grouping attributes are provided the method will respect the grouping of the receiver, or in cases of an
 * ungrouped receiver will simply count the rows in the data.frame
 *
 * @param selects The variables to to be used for cross-tabulation.
 * @param name The name of the count column resulting table.
 */
fun DataFrame.countExpr(vararg moreExpressions: TableExpression, name: String = "n", tableExpression: TableExpression? = null): DataFrame {
    val exprGrouped = groupByExpr(*moreExpressions, tableExpression = tableExpression).also { print(it) }
    return exprGrouped.count(*exprGrouped.groupedBy().names.toTypedArray(), name = name)
}

fun DataFrame.summarizeAt(columnSelect: ColumnSelector, vararg aggfuns: AggFun): DataFrame {
    return summarizeAt(columnSelect) {
        aggfuns.forEach { add(it.value, it.suffix) }
    }
}

data class AggFun(val value: SumFormula, val suffix: String? = null)

typealias SumFormula = DataCol.(DataCol) -> Any?
//typealias MutFormula = DataCol.(DataCol) -> Any?


/** Common ggregation Functions to be used along `summarizeAt/If/All*/
object SumFuns {
    val mean = AggFun({ mean() }, "mean")
    val median = AggFun({ mean() }, "median")
    val sd = AggFun({ sd() }, "sd")
    val n = AggFun({ length }, "n")
    val na = AggFun({ dataCol -> dataCol.isNA().filter { true }.size }, "na")
    // todo add rank etc
}

fun DataFrame.summarizeAt(columnSelect: ColumnSelector, op: (SummarizeBuilder.() -> Unit)? = null): DataFrame {
    return SummarizeBuilder(this, columnSelect).apply { op?.invoke(this) }.build()
}

class SummarizeBuilder(val df: DataFrame, val columnSelect: ColumnSelector) {
    val rules = emptyMap<SumFormula, String?>().toMutableMap()

    fun add(how: SumFormula, name: String? = null, separator: Char = '.') {
        rules[how] = separator.toString() + name
    }

    fun build(): DataFrame {

        val sumCols = df.select(columnSelect).names.toMutableList()
        if (df is GroupedDataFrame) {
            sumCols.removeAll(df.by)
        }

        val rules = sumCols.flatMap { colName: String ->
            rules.map { rule ->
                //todo use rule?.value as name instead
                val ruleName = colName + (rule.value ?: rule.key.hashCode())

                ColumnFormula(ruleName) { ec ->
                    val dataCol = ec[colName]
                    rule.key(dataCol, dataCol)
                }
            }
        }

        return df.summarize(*rules.toTypedArray())
    }
}

fun main(args: Array<String>) {
    //        irisData.summarizeEach({ startsWith("foo") },
    //            "mean" to { it["Species"].mean() },
    //            "median" to { it["Species"].mean() }
    //        )

    irisData.select { startsWith("Length") }.head().print()
    irisData.summarizeAt({ startsWith("Length") }) {
        add({ mean() }, "mean")
        add({ median() }, "median")
        //        mean() //todo add commom suspects
        //        median
    }


    println("-------")

    irisData.summarizeAt(
            { startsWith("Length") },
            SumFuns.mean,
            AggFun({ mean() }),
            AggFun({ median() })
    ).head().print()

}
////////////////////////////////////////////////
// groupBy() convenience
////////////////////////////////////////////////


/**
 * Creates a grouped data-frame from a column selector function. See `select()` for details about column selection.
 *
 * Most data operations are done on groups defined by variables. `group_by()` takes the receiver data-frame and
 * converts it into a grouped data-frame where operations are performed "by group". `ungroup()` removes grouping.
 *
 * Most krangl verbs like `addColumn()`, `summarize()`, etc. will be executed per group if a grouping is present.
 *
 * @sample krangl.samples.groupByExamples
 *
 */
fun DataFrame.groupBy(columnSelect: ColumnSelector): DataFrame = groupBy(*colSelectAsNames(columnSelect).toTypedArray())


/**
 * Creates a grouped data-frame from one or more table expressions. See `addColumn()` for details about table expressions.
 *
 * Most data operations are done on groups defined by variables. `group_by()` takes the receiver data-frame and
 * converts it into a grouped data-frame where operations are performed "by group". `ungroup()` removes grouping.
 *
 * Most krangl verbs like `addColumn()`, `summarize()`, etc. will be executed per group if a grouping is present.
 *
 * @sample krangl.samples.groupByExamples
 *
 */
fun DataFrame.groupByExpr(vararg moreExpressions: TableExpression, tableExpression: TableExpression? = null): DataFrame {

    val tableExpressions = (listOf(tableExpression) + moreExpressions).filterNotNull()
    val colFormulae = tableExpressions.mapIndexed { index, function ->
        "group_by_${index + 1}" to function
    }

    //    val extendedDf = allFormula.fold(this) { acc, next -> acc.addColumn(colFormulae) }
    return addColumns(*colFormulae.toTypedArray()).groupBy(*colFormulae.map { it.name }.toTypedArray())
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
        DataGroup(listOf(rowIndex), filter { BooleanArray(nrow, { index -> index == rowIndex }) })
    }.toList()

    return GroupedDataFrame(by = listOf("_row_"), groups = rowsAsGroups)
}


/**
 * Select rows by position while taking into account grouping in a data-frame.
 */
fun DataFrame.slice(vararg slices: Int) = filter { rowNumber.map { slices.contains(it) }.toBooleanArray() }


/**
 * Select rows by position while taking into account grouping in a data-frame.
 */
// note we do not reuse the impl from above to avoid expanding the ranges to full lists
fun DataFrame.slice(slice: IntRange) = filter { rowNumber.map { slice.contains(it) }.toBooleanArray() }

// note: supporting n() here seems pointless since nrow will also work in them mutate context


var PRINT_MAX_ROWS = 10
var PRINT_MAX_WIDTH = 100
var PRINT_MAX_DIGITS = 3
var PRINT_ROW_NUMBERS = true

/* Prints a dataframe to stdout. df.toString() will also work but has no options .*/
@JvmOverloads
fun DataFrame.print(
        title: String = "A DataFrame",
        colNames: Boolean = true,
        maxRows: Int = PRINT_MAX_ROWS,
        maxWidth: Int = PRINT_MAX_WIDTH,
        maxDigits: Int = PRINT_MAX_DIGITS,
        rowNumbers: Boolean = PRINT_ROW_NUMBERS
) = println(asString(title, colNames, maxRows, maxWidth, maxDigits, rowNumbers) + lineSeparator)


fun DataFrame.asString(
        title: String = "A DataFrame",
        colNames: Boolean = true,
        maxRows: Int = PRINT_MAX_ROWS,
        maxWidth: Int = PRINT_MAX_WIDTH,
        maxDigits: Int = PRINT_MAX_DIGITS,
        rowNumbers: Boolean = PRINT_ROW_NUMBERS
): String {

    var df = this

    if (this !is SimpleDataFrame) {
        df = this.ungroup() as SimpleDataFrame
    }

    // needed for smartcase
    if (df !is SimpleDataFrame) {
        throw UnsupportedOperationException()
    }

    val maxRowsOrInf = if (maxRows < 0) Integer.MAX_VALUE else maxRows
    val printData = take(Math.min(nrow, maxRowsOrInf))
            // optionally add rownames
            .run {
                if (rowNumbers) addColumn(" ") { rowNumber }.moveLeft(" ") else this
            }

    val valuePrinter = createValuePrinter(maxDigits)

    // calculate indents
    val colWidths = printData.cols.map { it.values().map { (valuePrinter(it)).length }.max() ?: 20 }
    val headerWidths = printData.names.map { it.length }


    // detect column padding
    val columnSpacing = 3
    val padding = colWidths.zip(headerWidths)
            .map { (col, head) -> listOf(col, head).max()!! + columnSpacing }
            // remove spacer from first column to have correction alignment with beginning of line
            .toMutableList().also { if (it.size > 0) it[0] -= columnSpacing }.toList()


    // do the actual printing
    val sb = StringBuilder()

    sb.appendln("${title}: ${nrow} x ${ncol}")

    if (this is GroupedDataFrame) {
        sb.appendln("Groups: ${by.joinToString()} [${groups.size}]")
    }


    // determine which column to actually print to obey width limitations
    //    val numPrintCols = 300
    val numPrintCols = padding.asSequence()
            .scanLeft(0) { acc, next -> acc + next }
            .withIndex().takeWhile { it.value < maxWidth }
            .last().index

    val widthTrimmed = printData.select(printData.names.take(numPrintCols))


    if (colNames) widthTrimmed.cols.mapIndexed { index, col ->
        col.name.padStart(padding[index])
    }.joinToString("").apply {
        sb.appendln(this)
    }


    widthTrimmed.rows.map { it.values }.map { rowData ->
        // show null as NA when printing data
        rowData.mapIndexed { index, value ->
            valuePrinter(value).padStart(padding[index])
        }.joinToString("").apply { sb.appendln(this) }
    }

    // similar to dplyr render a summary below the table
    var and: List<String> = emptyList()
    if (maxRowsOrInf < df.nrow) {
        and += "and ${df.nrow - maxRowsOrInf} more rows"
    }

    if (numPrintCols < printData.ncol) {
        val leftOutCols = printData.select(names.subList(numPrintCols, names.size))
        and += "" + "and ${printData.ncol - numPrintCols} more variables: ${leftOutCols.names.joinToString()}"
    }
    sb.append(and.joinToString(", and ").wrap(maxWidth))

    return sb.trim().toString()
}


// from http://www.davismol.net/2015/02/03/java-how-to-split-a-string-into-fixed-length-rows-without-breaking-the-words/
private fun String.wrap(lineSize: Int): String {
    val res = ArrayList<String>()

    val p = Pattern.compile("\\b.{1," + (lineSize - 1) + "}\\b\\W?")
    val m = p.matcher(this)

    while (m.find()) {
        //        System.out.println(m.group().trim())   // Debug
        res.add(m.group().trim())
    }
    return res.joinToString(lineSeparator)
}

data class ColSpec(val pos: Int, val name: String, val type: String)


//note we intentially use a parameter instead of a receiver here to minimize public extension api
fun columnTypes(df: DataFrame): List<ColSpec> {
    if (df is GroupedDataFrame) return columnTypes(df.ungroup())

    return df.cols.mapIndexed { index, col -> ColSpec(index, col.name, getColumnType(col)) }
}

fun List<ColSpec>.asDf() = deparseRecords { mapOf("index" to it.pos, "name" to it.name, "type" to it.type) }

fun List<ColSpec>.print() = asDf().print()


// see https://spark.apache.org/docs/latest/sql-programming-guide.html#untyped-dataset-operations-aka-dataframe-operations
/**
 *  Prints the schema (that is column names, types, and the first few values per column) of a dataframe to stdout.
 */
fun DataFrame.schema(maxDigits: Int = 3, maxWidth: Int = PRINT_MAX_WIDTH) {
    if (this is GroupedDataFrame) {
        ungroup().schema(maxDigits, maxWidth)
        return
    }

    val topN = this
    println("DataFrame with ${nrow} observations")

    val namePadding = topN.cols.map { it.name.length }.max() ?: 0

    val typeLabels = topN.cols.map { col -> getColumnType(col, wrapSquares = true) }

    val typePadding = typeLabels.map { it.length }.max() ?: 0

    topN.cols.zip(typeLabels).forEach { (col, typeLabel) ->
        val stringifiedVals = col.values().take(255).asSequence()
                .joinToMaxLengthString(maxLength = maxWidth, transform = createValuePrinter(maxDigits))

        println("${col.name.padEnd(namePadding)}  ${typeLabel.padEnd(typePadding)}  $stringifiedVals")
    }
}

internal fun getColumnType(col: DataCol, wrapSquares: Boolean = false): String {
    return when (col) {
        is DoubleCol -> "Dbl"
        is IntCol -> "Int"
        is LongCol -> "Long"
        is StringCol -> "Str"
        is BooleanCol -> "Bol"
        is AnyCol -> guessAnyType(col)
        else -> throw UnsupportedOperationException()
    }.let { if (wrapSquares) "[$it]" else it }
}


private fun guessAnyType(col: AnyCol): String {
    val firstEl = col.values.asSequence().filterNotNull().firstOrNull()

    if (firstEl == null) return "Any"

    return firstEl.javaClass.simpleName
            // tweak types for nested data
            .replace("SimpleDataFrame", "DataFrame")
            .replace("GroupedDataFrame", "DataFrame")
            // take care of data-classes defined in other classes or methods
            .replace(".*\\$".toRegex(), "")
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


/** Adds new rows. Missing entries are set to null. The output of bindRows() will contain a column if that column appears in any of the inputs.
 *
 * When row-binding, columns are matched by name, and any missing columns will be filled with NA.
 *
 * Grouping will be discarded when binding rows.
 *
 * @sample krangl.samples.bindRowsExamples
 */
fun List<DataFrame>.bindRows(): DataFrame {
    return bindRows(*toTypedArray())
}


/** Adds new rows. Missing entries are set to null. The output of bindRows() will contain a column if that column appears in any of the inputs.
 *
 * When row-binding, columns are matched by name, and any missing columns will be filled with NA.
 *
 * Grouping will be discarded when binding rows.
 *
 * @sample krangl.samples.bindRowsExamples
 */
fun DataFrame.bindRows(df: DataFrame): DataFrame = bindRows(this, df)


/** Adds new rows. Missing entries are set to null. The output of bindRows() will contain a column if that column appears in any of the inputs.
 *
 * When row-binding, columns are matched by name, and any missing columns will be filled with NA.
 *
 * Grouping will be discarded when binding rows.
 *
 * @sample krangl.samples.bindRowsExamples
 */
fun DataFrame.bindRows(vararg someRows: DataFrameRow): DataFrame =
        bindRows(this, dataFrameOf(listOf(*someRows)))


/** Adds new rows. Missing entries are set to null. The output of bindRows() will contain a column if that column appears in any of the inputs.
 *
 * When row-binding, columns are matched by name, and any missing columns will be filled with NA.
 *
 * Grouping will be discarded when binding rows.
 *
 * @sample krangl.samples.bindRowsExamples
 */
fun bindRows(vararg dataFrames: DataFrame): DataFrame { // add options about NA-fill over non-overlapping columns
    val bindCols = mutableListOf<DataCol>()

    val colNames = dataFrames
            .map { it.names }
            .foldRight(emptyList<String>()) { acc, right ->
                acc + right.minus(acc)
            }

    for (colName in colNames) {
        val colDataCombined: Array<*> = bindColData(dataFrames.toList(), colName)

        // tbd: seems cleaner&better but fails for some reason
        //        bindCols.add(handleArrayErasure(colName, colDataCombined))

        when (dataFrames.first { it.names.contains(colName) }[colName]) {
            is DoubleCol -> DoubleCol(colName, colDataCombined.map { it as Double? })
            is IntCol -> IntCol(colName, colDataCombined.map { it as Int? })
            is LongCol -> LongCol(colName, colDataCombined.map { it as Long? })
            is StringCol -> StringCol(colName, colDataCombined.map { it as String? })
            is BooleanCol -> BooleanCol(colName, colDataCombined.map { it as Boolean? })
            is AnyCol -> AnyCol(colName, colDataCombined.toList())
            else -> throw UnsupportedOperationException()
        }.apply { bindCols.add(this) }
    }

    return SimpleDataFrame(bindCols)
}

internal class DuplicateNameResolver(val names: List<String>) {
    fun resolve(colName: String): String {
        if (!names.contains(colName)) return colName

        for (suffix in 1..Int.MAX_VALUE) {
            with(colName + "_" + suffix) {
                if (!names.contains(this)) return this
            }
        }

        throw IllegalArgumentException()
    }
}


fun Iterable<DataCol>.bindCols(): DataFrame { // add options about NA-fill over non-overlapping columns
    return dataFrameOf(*this.toList().toTypedArray())
}

//fun main(args: Array<String>) {
//    listOf(DoubleCol("foo", doubleArrayOf(1.2))).bindCols()
//}

// todo we should use a more deambiguation approach here,
// like in:  if bla_1 is also present, Use bla_2, and so on
fun bindCols(left: DataFrame, right: DataFrame, renameDuplicates: Boolean = true): DataFrame { // add options about NA-fill over non-overlapping columns
    val duplicatedNames = right.names.intersect(left.names)

    @Suppress("NAME_SHADOWING")
    val right = if (renameDuplicates) {
        val nameResolver = DuplicateNameResolver(left.names)

        right.rename(*duplicatedNames.map {
            RenameRule(it, nameResolver.resolve(it))
        }.toTypedArray())
    } else {
        right
    }

    return SimpleDataFrame(left.cols.toMutableList().apply { addAll((right as SimpleDataFrame).cols) })
}

private fun bindColData(dataFrames: List<DataFrame>, colName: String): Array<*> {
    val totalRows = dataFrames.map { it.nrow }.sum()

    val arrayList = Array<Any?>(totalRows, { 0 })

    var iter = 0

    dataFrames.forEach {
        if (it.names.contains(colName)) {
            it[colName].values().forEach {
                arrayList[iter++] = it
            }
        } else {
            // colunn is missing in `it`
            for (row in (0 until it.nrow)) {
                arrayList[iter++] = null
            }
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
        groups.map { DataGroup(it.groupKey, trafo(it.df)) }.let { GroupedDataFrame(by, it) }


fun emptyDataFrame(): DataFrame = SimpleDataFrame()


/** Return an iterator over the rows in data in the receiver. */
internal fun DataFrame.rowData(): Iterable<List<Any?>> = when (this) {

    is GroupedDataFrame -> throw UnsupportedOperationException()
    is SimpleDataFrame -> object : Iterable<List<Any?>> {

        override fun iterator() = object : Iterator<List<Any?>> {

            val colIterators = cols.map { it.values().iterator() }

            override fun hasNext(): Boolean = colIterators.firstOrNull()?.hasNext() ?: false

            override fun next(): List<Any?> = colIterators.map { it.next() }
        }
    }

    else -> throw IllegalArgumentException()
}


internal val DataFrame.rowNumber: List<Int> get() = (1..nrow).toList()


fun DataFrame.toDoubleMatrix(): Array<DoubleArray> {
    selectIf { !(it is IntCol || it is LongCol || it is DoubleCol) }.names.let {
        require(it.isEmpty()) { "Can not cast to double matrix because not all columns are numeric" }
    }

    val matrix = cols.map {
        when (it) {
            is DoubleCol -> it.values.requireNoNulls().toDoubleArray()
            is IntCol -> it.values.requireNoNulls().map { it.toDouble() }.toDoubleArray()
            is LongCol -> it.values.requireNoNulls().map { it.toDouble() }.toDoubleArray()
            else -> TODO()
        }
    }.toTypedArray()


    // todo should we transpose the matrix by default?
    // https://stackoverflow.com/questions/15449711/transpose-double-matrix-with-a-java-function

    return matrix
}


fun DataFrame.toFloatMatrix(): Array<FloatArray> = toDoubleMatrix().toFloatMatrix()

private fun Array<DoubleArray>.toFloatMatrix(): Array<FloatArray> = map { it.map { it.toFloat() }.toFloatArray() }.toTypedArray()


class Factor(val index: Int, val values: Array<String>)

fun DataCol.asFactor(): DataCol {
    return when (this) {
        is StringCol -> {
            val levels = this.values.filterNotNull().distinct().toTypedArray()
            val factorizedValues = values.mapNonNull { Factor(levels.indexOf(it), levels) }
            AnyCol(name, factorizedValues)
        }
        else -> TODO()
    }
}

internal val lineSeparator = System.getProperty("line.separator")
