@file:JvmName("Extensions")

package krangl

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


typealias TableExpression = DataFrame.(DataFrame) -> Any?

// as would also prevent us from overwriting to
//infix fun String.to(that: TableExpression) = Pair<String, DataFrame.(DataFrame) -> Any?>(this, that)

infix fun String.to(that: TableExpression) = ColumnFormula(this, that)


data class ColumnFormula(val name: String, val expression: TableExpression)

////////////////////////////////////////////////
// filter() convenience
////////////////////////////////////////////////

/** Filter the rows of a table with a single predicate.*/

fun DataFrame.filter(predicate: DataFrame.(DataFrame) -> List<Boolean>): DataFrame = filter({ predicate(this).toBooleanArray() })

/** AND-filter a table with different filters.*/
fun DataFrame.filter(vararg predicates: DataFrame.(DataFrame) -> List<Boolean>): DataFrame =
        predicates.fold(this, { df, p -> df.filter(p) })

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

    return addColumns(*sortBys.toTypedArray()).
            sortedBy(*sortByNames).
            remove(sortByNames.asList())
    //           select({ oneOf(*sortByNames).not() })
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


/** Counts observations by group.*/
fun DataFrame.count(vararg selects: String = this.names.toTypedArray(), countName: String = "n"): DataFrame = select(*selects).groupBy(*selects).summarize(countName, { nrow })


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
fun DataFrame.print(colNames: Boolean = true, maxRows: Int = 20) = println(asString(colNames, maxRows))


fun DataFrame.asString(colNames: Boolean = true, maxRows: Int = 20): String {

    var df = this

    if (this !is SimpleDataFrame) {
        df = this.ungroup() as SimpleDataFrame
    }

    // needed for smartcase
    if (df !is SimpleDataFrame) {
        throw UnsupportedOperationException()
    }

    val printData = take(Math.max(nrow, maxRows))

    // calculate indents
    val colWidths = printData.cols.map { it.values().map { (it ?: "<NA>").toString().length }.max() ?: 20 }
    val headerWidths = printData.names.map { it.length }
    val padding = colWidths.zip(headerWidths).map { (col, head) -> listOf(col, head).max()!! + 3 }


    val sb = StringBuilder()

    if (colNames) df.cols.mapIndexed { index, col ->
        col.name.padStart(padding[index])
    }.joinToString("").apply {
        sb.appendln(this)
    }

    printData.rows.map { it.values }.map { rowData ->
        // show null as NA when printing data
        rowData.mapIndexed { index, value ->
            (value?.toString() ?: "<NA>").padStart(padding[index])
        }.joinToString("").apply { sb.appendln(this) }
    }

    return sb.toString()
}


data class ColSpec(val pos: Int, val name: String, val type: String)


internal fun getColType(col: DataCol) = when (col) {
    is AnyCol -> col.values.first()?.javaClass?.simpleName
    else -> col.javaClass.simpleName.replace("Col", "")
}


fun DataFrame.structure(): List<ColSpec> {
    // todo add support for grouped data here
    if (this !is SimpleDataFrame) {
        TODO()
    }

    return cols.mapIndexed { index, col -> ColSpec(index, col.name, getColType(col) ?: "") }
}

fun List<ColSpec>.asDf() = asDataFrame { mapOf("index" to it.pos, "name" to it.name, "type" to it.type) }

fun List<ColSpec>.print() = asDf().print()


/* Prints the structure of a dataframe to stdout.*/
fun DataFrame.glimpse(sep: String = "\t") {
    // todo add support for grouped data here
    if (this !is SimpleDataFrame) {
        return
    }

    val topN = take(8) as SimpleDataFrame
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
fun DataFrame.printDataClassSchema(varName: String, dataClassName: String = varName.capitalize()) {
    val df = this.ungroup() as SimpleDataFrame

    // create type
    val dataSpec = df.cols.map { "val ${it.name}: ${getScalarColType(it)}" }.joinToString(", ")
    println("data class ${dataClassName}(${dataSpec})")

    // map dataframe to
    // example: val dfEntries = df.rows.map {row ->  Df(row.get("first_name") as String ) }

    val attrMapping = df.cols.map { """ row["${it.name}"] as ${getScalarColType(it)}""" }.joinToString(", ")

    println("val records = ${varName}.rows.map { row -> ${dataClassName}(${attrMapping}) }")
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
fun DataFrame.const(someThing: Any) = anyAsColumn(someThing, TMP_COLUMN, nrow)

internal inline fun warning(value: Boolean, lazyMessage: () -> Any): Unit {
    if (!value) System.err.println(lazyMessage())
}

internal fun GroupedDataFrame.transformGroups(trafo: (DataFrame) -> DataFrame): GroupedDataFrame =
        groups.map { DataGroup(it.groupHash, trafo(it.df)) }.let { GroupedDataFrame(by, it) }


fun List<DataCol>.asDataFrame(): DataFrame = SimpleDataFrame(this)


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

