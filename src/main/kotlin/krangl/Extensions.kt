@file:JvmName("Extensions")

package krangl

import java.util.*


////////////////////////////////////////////////
// select() helpers and API
////////////////////////////////////////////////

class ColNames(val names: List<String>)

// select utitlies see http://www.rdocumentation.org/packages/dplyr/functions/select
fun ColNames.matches(regex: String) = names.map { it.matches(regex.toRegex()) }

// todo add possiblity to negative selection with mini-language. E.g. -startsWith("name")

internal fun List<Boolean>.falseAsNull() = map { if (!it) null else true }
internal fun List<Boolean>.trueAsNull() = map { if (it) null else false }
internal fun List<Boolean?>.nullAsFalse(): List<Boolean> = map { it ?: false }

fun ColNames.startsWith(prefix: String) = names.map { it.startsWith(prefix) }
fun ColNames.endsWith(prefix: String) = names.map { it.endsWith(prefix) }.falseAsNull()
fun ColNames.everything() = Array(names.size, { true }).toList()
fun ColNames.matches(regex: Regex) = names.map { it.matches(regex) }.falseAsNull()
fun ColNames.oneOf(vararg someNames: String): List<Boolean?> = names.map { someNames.contains(it) }.falseAsNull()
fun ColNames.oneOf(someNames: List<String>): List<Boolean?> = names.map { someNames.contains(it) }.falseAsNull()


fun ColNames.range(from: String, to: String): List<Boolean?> {
    val rangeStart = names.indexOf(from)
    val rangeEnd = names.indexOf(to)

    val rangeSelection = (rangeStart..rangeEnd).map { names[it] }
    return names.map { rangeSelection.contains(it) }.falseAsNull()
}


// since this affects String namespace it might be not a good idea
operator fun String.unaryMinus() = fun ColNames.(): List<Boolean?> = names.map { it != this@unaryMinus }.trueAsNull()

operator fun Iterable<String>.unaryMinus() = fun ColNames.(): List<Boolean> = names.map { !this@unaryMinus.contains(it) }
operator fun List<Boolean?>.unaryMinus() = map { it?.not() }

//val another = -"dsf"

/** Convenience wrapper around to work with varag <code>krangl.DataFrame.select</code> */
fun DataFrame.select(vararg columns: String): DataFrame = select(columns.asList())

/** Keeps only the variables that match any of the given expressions. E.g. use `startsWith("foo")` to select for columns staring with 'foo'.*/
fun DataFrame.select(vararg which: ColNames.() -> List<Boolean?>): DataFrame {
    val reducedSelector = reduceColSelectors(which)

    return select(reducedSelector)
}

internal fun DataFrame.reduceColSelectors(which: Array<out ColNames.() -> List<Boolean?>>): List<Boolean?> {
    val reducedSelector = which.map { it(ColNames(names)) }.reduce { selA, selB -> selA nullAwareAND  selB }
    return reducedSelector
}

internal fun DataFrame.select(which: List<Boolean?>): DataFrame {
    val colSelection: List<String> = colSelectAsNames(which)

    return select(colSelection)
}

internal fun DataFrame.colSelectAsNames(which: List<Boolean?>): List<String> {
    require(which.size == ncol) { "selector array has different dimension than data-frame" }

    // map boolean array to string selection
    val isPosSelection = which.count { it == true } > 0
    val whichComplete = which.map { it ?: !isPosSelection }

    val colSelection: List<String> = names
            .zip(whichComplete)
            .filter { it.second }.map { it.first }

    return colSelection
}


//private infix fun List<Boolean?>.nullOR(other: List<Boolean?>): List<Boolean?> = mapIndexed { index, first ->
//    if(first==null && other[index] == null) null else (first  ?: false) || (other[index] ?: false)
//}
private infix fun List<Boolean?>.nullAwareAND(other: List<Boolean?>): List<Boolean?> = this.zip(other).map {
    it.run {
        if (first == null && second == null) {
            null
        } else if (first != null && second != null) {
            20
            first!! && second!!
        } else {
            first ?: second
        }
    }
}


//
// Data Reshaping  
//

data class RenameRule(val oldName: String, val newName: String) {
    fun asTableFormula() = TableFormula(newName, { df -> df[oldName] })
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
    val renamed = old2NewFilt.fold(this, { df, renRule -> df.createColumn(renRule.asTableFormula()).select(-renRule.oldName) })


    // restore positions of renamed columns
    return renamed.select(*namesRestoredPos.toTypedArray())
}


//
// mutate() convenience
//


// as would also prevent us from overwriting to
//infix fun String.to(that: DataFrame.(DataFrame) -> Any?) = Pair<String, DataFrame.(DataFrame) -> Any?>(this, that)

infix fun String.to(that: DataFrame.(DataFrame) -> Any?) = TableFormula(this, that)


data class TableFormula(val resultName: String, val formula: DataFrame.(DataFrame) -> Any?)

fun DataFrame.createColumn(columnName: String, expression: DataFrame.(DataFrame) -> Any?): DataFrame =
        createColumn(columnName to expression)

fun DataFrame.createColumns(vararg mutations: TableFormula): DataFrame {
    return mutations.fold(this, { df, tf -> df.createColumn(tf) })
}

/** Mutates a data-frame and discards all non-result columns. */
fun DataFrame.transmute(vararg formula: TableFormula) = createColumns(*formula).select(*formula.map { it.resultName }.toTypedArray())


////////////////////////////////////////////////
// filter() convenience
////////////////////////////////////////////////

/** Filter the rows of a table with a single predicate.*/

fun DataFrame.filter(predicate: DataFrame.(DataFrame) -> List<Boolean>): DataFrame = filter({ predicate(this).toBooleanArray() })

/** AND-filter a table with different filters.*/
fun DataFrame.filter(vararg predicates: DataFrame.(DataFrame) -> List<Boolean>): DataFrame =
        predicates.fold(this, { df, p -> df.filter (p) })

// // todo does not work why?
// df.filter({ it["last_name"].asStrings().map { it!!.startsWith("Do") } })


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


/** Random number generator used to row sampling. Reassign to set seed for deterministic sampling. */
var _rand = Random(3) // use var here to allow users to set seeds in order to do deterministic sampling

////////////////////////////////////////////////
// summarize() convenience
////////////////////////////////////////////////

fun DataFrame.summarize(name: String, formula: DataFrame.(DataFrame) -> Any?): DataFrame = summarize(name to formula)


//fun DataFrame.count(name: String, formula: DataFrame.(DataFrame) -> Any?): DataFrame = summarize(name to formula)

/** Retain only unique/distinct rows from an input tbl.
 *
 * @arg selects: Variables to use when determining uniqueness. If there are multiple rows for a given combination of inputs, only the first row will be preserved.
 */
// todo provide more efficient implementation
fun DataFrame.distinct(vararg selects: String = this.names.toTypedArray()): DataFrame =
        groupBy(*selects).slice(1).ungroup()


/** Counts observations by group.*/
fun DataFrame.count(vararg selects: String = this.names.toTypedArray(), countName: String = "n"): DataFrame = select(*selects).groupBy(*selects).summarize(countName, { nrow })


////////////////////////////////////////////////
// General Utilities
////////////////////////////////////////////////


// Extension function that mimic other major elements of the dplyr API

//fun DataFrame.rowNumber() = IntCol(TMP_COLUMN, (1..nrow).asSequence().toList())
//fun DataFrame.rowNumber() = IntCol("row_number", (1..nrow).toList() )

fun DataFrame.head(numRows: Int = 5) = filter {
    rowNumber.map { it <= numRows }.toBooleanArray()
}

fun DataFrame.tail(numRows: Int = 5) = filter { rowNumber.map { it > (nrow - numRows) }.toBooleanArray() }


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
@JvmOverloads fun DataFrame.print(colNames: Boolean = true, sep: String = "\t") = println(asString(colNames, sep))


fun DataFrame.asString(colNames: Boolean = true, sep: String = "\t", maxRows: Int = 100): String {

    var df = this

    if (this !is SimpleDataFrame) {
        df = this.ungroup() as SimpleDataFrame
    }

    // needed for smartcase
    if (df !is SimpleDataFrame) {
        throw UnsupportedOperationException()
    }


    val sb = StringBuilder()

    if (colNames) df.cols.map { it.name }.joinToString(sep).apply { sb.appendln(this) }

    rawRows.take(Math.min(nrow, maxRows)).map { row: List<Any?> ->
        // show null as NA when printing data
        row.map { it ?: "<NA>" }.joinToString(sep).apply { sb.appendln(this) }
    }

    return sb.toString()
}

/* Prints the structure of a dataframe to stdout.*/
fun DataFrame.glimpse(sep: String = "\t") {
    // todo add support for grouped data here
    if (this !is SimpleDataFrame) {
        return
    }

    val topN = head(8) as SimpleDataFrame
    println("DataFrame with ${nrow} observations")

    for (col in topN.cols) {
        when (col) {
            is DoubleCol -> listOf("[Dbl]\t", col.values.toList())
            is IntCol -> listOf("[Int]\t", col.values.toList())
            is StringCol -> listOf("[Str]\t", col.values.toList())
            is BooleanCol -> listOf("[Bol]\t", col.values.toList())
            is AnyCol -> listOf("[Any]\t", col.values.toList())
            else -> throw UnsupportedOperationException()
        }.joinToString(", ", prefix = col.name + "\t: ").apply { println(this) }
    }
}

/** Provides a code to convert  a dataframe to a strongly typed list of kotlin data-class instances.*/
fun DataFrame.toKotlin(dfVarName: String, dataClassName: String = dfVarName.capitalize()) {
    val df = this.ungroup() as SimpleDataFrame

    // create type
    val dataSpec = df.cols.map { "val ${it.name}: ${getScalarColType(it) }" }.joinToString(", ")
    println("data class ${dataClassName}(${dataSpec})")

    // map dataframe to
    // example: val dfEntries = df.rows.map {row ->  Df(row.get("first_name") as String ) }

    val attrMapping = df.cols.map { """ row["${it.name}"] as ${getScalarColType(it)}""" }.joinToString(", ")

    println("val ${dfVarName}Entries = ${dfVarName}.rows.map { row -> ${dataClassName}(${attrMapping}) }")
}


/** Concatenate a list of data-frame by row. */
fun List<DataFrame>.bindRows(): DataFrame { // add options about NA-fill over non-overlapping columns
    // todo more column model consistency checks here
    // note: use fold to bind with non-overlapping column model

    val bindCols = mutableListOf<DataCol>()

    val totalRows = map { it.nrow }.sum()

    for (colName in this.firstOrNull()?.names ?: emptyList()) {
        val colDataCombined: Array<*> = bindColData(colName)

        when (this.first()[colName]) {
            is DoubleCol -> DoubleCol(colName, colDataCombined.map { it as Double ? })
            is IntCol -> IntCol(colName, colDataCombined.map { it as Int ? })
            is StringCol -> StringCol(colName, colDataCombined.map { it as String ? })
            is BooleanCol -> BooleanCol(colName, colDataCombined.map { it as Boolean ? })
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
fun DataFrame.const(someThing: Any) = anyAsColumn(someThing, TMP_COLUMN, nrow)

internal inline fun warning(value: Boolean, lazyMessage: () -> Any): Unit {
    if (!value) System.err.println(lazyMessage())
}

internal fun GroupedDataFrame.transformGroups(trafo: (DataFrame) -> DataFrame): GroupedDataFrame =
        groups.map { DataGroup(it.groupHash, trafo(it.df)) }.let { GroupedDataFrame(by, it) }
