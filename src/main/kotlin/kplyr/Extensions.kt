package kplyr


// as would also prevent us from overwriting to
//infix fun String.to(that: DataFrame.(DataFrame) -> Any?) = Pair<String, DataFrame.(DataFrame) -> Any?>(this, that)

infix fun String.to(that: DataFrame.(DataFrame) -> Any?) = TableFormula(this, that)


////////////////////////////////////////////////
// select() helpers and API
////////////////////////////////////////////////

class ColNames(val names: List<String>)

// select utitlies see http://www.rdocumentation.org/packages/dplyr/functions/select
fun ColNames.matches(regex: String) = names.map { it.matches(regex.toRegex()) }

// todo add possiblity to negate selection with mini-language. E.g. -startsWith("name")
fun ColNames.startsWith(prefix: String) = names.map { it.startsWith(prefix) }

fun ColNames.endsWith(prefix: String) = names.map { it.endsWith(prefix) }
fun ColNames.everything() = Array(names.size, { true }).toList()
fun ColNames.oneOf(vararg someNames: String) = Array(names.size, { someNames.contains(names[it]) }).toList()


// since this affects String namespace it might be not a good idea
operator fun String.unaryMinus() = fun ColNames.(): List<Boolean> = names.map { it != this@unaryMinus }
//val another = -"dsf"

/** Keeps only the variables you mention.*/
//

fun DataFrame.select(vararg columns: String): DataFrame {
    if (columns.isEmpty()) System.err.println("Calling select() without arguments is not sensible")
    return select(this.names.map { colName -> columns.contains(colName) })
}

/** Keeps only the variables that match any of the given expressions. E.g. use `startsWith("foo")` to select for columns staring with 'foo'.*/
fun DataFrame.select(vararg which: ColNames.() -> List<Boolean>): DataFrame {
    return which.drop(1).fold(which.first()(ColNames(names)), {
        initial, next ->
        initial OR next(ColNames(names))
    }).let { select(it.toList()) }
}


fun DataFrame.rename(vararg old2new: Pair<String, String>): DataFrame {
//    renames.map{ it.first to  { df -> df[it.second]} }
    val changeList = old2new.map { TableFormula(it.first, { df -> df[it.second] }) }
    return this.mutate(*changeList.toTypedArray())
}

////////////////////////////////////////////////
// filter() convenience
////////////////////////////////////////////////

// todo implement transmute() extension function
//fun DataFrame.transmute(formula: DataFrame.(DataFrame) -> Any): DataFrame = throw UnsupportedOperationException()

// vararg support for mutate
data class TableFormula(val resultName: String, val formula: DataFrame.(DataFrame) -> Any?)


//data class RenameRule(val oldName:String, val newName:String)


fun DataFrame.mutate(vararg mutations: TableFormula): DataFrame {
    return mutations.fold(this, { df, tf -> df.mutate(tf) })
}


// mutate convenience
fun DataFrame.filter(predicate: DataFrame.(DataFrame) -> List<Boolean>): DataFrame = filter({ predicate(this).toBooleanArray() })
// // todo does not work why?
// df.filter({ it["last_name"].asStrings().map { it!!.startsWith("Do") } })


////////////////////////////////////////////////
// summarize() convenience
////////////////////////////////////////////////

fun DataFrame.summarize(name: String, formula: DataFrame.(DataFrame) -> Any?): DataFrame = summarize(name to formula)


fun DataFrame.count(name: String, formula: DataFrame.(DataFrame) -> Any?): DataFrame = summarize(name to formula)

/** Retain only unique/distinct rows from an input tbl.
 *
 * @arg selects: Variables to use when determining uniqueness. If there are multiple rows for a given combination of inputs, only the first row will be preserved.
 */
// todo provide more efficient implementation
fun DataFrame.distinct(vararg selects: String = this.names.toTypedArray()): DataFrame =
        groupBy(*selects).slice(1).ungroup()


/** Counts observations by group.*/
fun DataFrame.count(vararg selects: String = this.names.toTypedArray(), colName: String = "n"): DataFrame =
        select(*selects).groupBy(*selects).summarize(colName, { nrow })


////////////////////////////////////////////////
// General Utilities
////////////////////////////////////////////////


// Extension function that mimic other major elements of the dplyr API

//fun DataFrame.rowNumber() = IntCol(TMP_COLUMN, (1..nrow).asSequence().toList())
fun DataFrame.rowNumber() = (1..nrow).asIterable()

fun DataFrame.head(numRows: Int = 5) = filter { rowNumber().map { it <= numRows }.toBooleanArray() }
fun DataFrame.tail(numRows: Int = 5) = filter { rowNumber().map { it > (nrow - numRows) }.toBooleanArray() }


/* Select rows by position.
 * Similar to dplyr::slice this operation works in a grouped manner.
 */
fun DataFrame.slice(vararg slices: Int) = filter { rowNumber().map { slices.contains(it) }.toBooleanArray() }

// note: supporting n() here seems pointless since nrow will also work in them mutate context


/* Prints a dataframe to stdout. df.toString() will also work but has no options .*/
fun DataFrame.print(colNames: Boolean = true, sep: String = "\t") = println(asString(colNames, sep))


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

    (1..Math.min(nrow, maxRows)).map { df.row(it - 1).values.joinToString(sep).apply { sb.appendln(this) } }

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
            is StringCol -> listOf("[Str]\t", col.values)
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
        val colDataCombined = Array(totalRows, { bindColData(colName)[it] })

        when (this.first()[colName]) {
            is DoubleCol -> DoubleCol(colName, colDataCombined.map { it as Double ? })
            is IntCol -> IntCol(colName, colDataCombined.map { it as Int ? })
            is StringCol -> StringCol(colName, colDataCombined.map { it as String ? })
            is BooleanCol -> BooleanCol(colName, colDataCombined.map { it as Boolean ? })
            is AnyCol<*> -> AnyCol<Any>(colName, colDataCombined.toList())
            else -> throw UnsupportedOperationException()
        }.apply { bindCols.add(this) }

    }

    return SimpleDataFrame(bindCols)
}

fun bindCols(left: DataFrame, right: DataFrame): DataFrame { // add options about NA-fill over non-overlapping columns
    return SimpleDataFrame((left as SimpleDataFrame).cols.toMutableList().apply { addAll((right as SimpleDataFrame).cols) })
}

private fun List<DataFrame>.bindColData(colName: String): List<*> {
    val groupsData: List<List<*>> = map { it[colName].values() }
    return groupsData.reduce { accu, curEl -> accu.toMutableList().apply { addAll(curEl) }.toList() }
}

// Misc or TBD


//fun Any?.asCol(df:DataFrame) = anyAsColumn(this, TMP_COLUMN, df.nrow)

// Any.+ overloading does not work and is maybe neight a good idea since it's affecting global operator conventions
//infix operator fun Any.plus(rightCol:DataCol) = anyAsColumn(this, TMP_COLUMN, rightCol.values().size) + rightCol
fun DataFrame.const(someThing: Any) = anyAsColumn(someThing, TMP_COLUMN, nrow)

