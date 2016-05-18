@file:Suppress("unused")

package kplyr


// as would also prevent us from overwriting to
infix fun String.to(that: DataFrame.(DataFrame) -> Any?) = Pair<String, DataFrame.(DataFrame) -> Any?>(this, that)


interface DataFrame {

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


    // Core Manipulation Verbs

    /** select() keeps only the variables you mention.*/
    fun select(which: List<Boolean>): DataFrame

    // todo consider to use List<Boolean> in signature. We can not provide both because of type erasure
    fun filter(predicate: DataFrame.(DataFrame) -> BooleanArray): DataFrame

    /** Mutate adds new variables and preserves existing.*/
    fun mutate(name: String, formula: DataFrame.(DataFrame) -> Any?): DataFrame
    // todo maybe as would be more readible: df.mutate({ mean(it["foo")} as "mean_foo")
    // todo Also support varargs similar to summarize: var newDf = df.mutate({"new_attr" to  ( it["test"] + it["test"] )})


    // todo also support mini-lang in arrange(); eg: df.arrange(desc("foo"))
    fun arrange(vararg by: String): DataFrame

    fun summarize(vararg sumRules: Pair<String, DataFrame.(DataFrame) -> Any?>): DataFrame

    fun groupBy(vararg by: String): DataFrame

    fun ungroup(): DataFrame
}


////////////////////////////////////////////////
// select() helpers and API
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


// todo implement rename() extension function

////////////////////////////////////////////////
// filter() convenience
////////////////////////////////////////////////

// todo implement transmute() extension function
//fun DataFrame.transmute(formula: DataFrame.(DataFrame) -> Any): DataFrame = throw UnsupportedOperationException()


// mutate convenience
fun DataFrame.filter(predicate: DataFrame.(DataFrame) -> List<Boolean>): DataFrame = filter({ predicate(this).toBooleanArray() })
// // todo does not work why?
// df.filter({ it["last_name"].asStrings().map { it!!.startsWith("Do") } })


////////////////////////////////////////////////
// summarize() convenience
////////////////////////////////////////////////

fun DataFrame.summarize(name: String, formula: DataFrame.(DataFrame) -> Any?): DataFrame = summarize(name to formula)


////////////////////////////////////////////////
// General Utilities
////////////////////////////////////////////////


// Extension function that mimic othe major elements of the dplyr API

fun DataFrame.head(numRows: Int = 5) = filter { rowNumber() lt numRows }
fun DataFrame.tail(numRows: Int = 5) = filter { rowNumber() gt (nrow - numRows) }
fun DataFrame.rowNumber() = IntCol(TMP_COLUMN, (1..nrow).asSequence().toList())
// note: supporting n() here seems pointless since nrow will also work in them mutate context



/* Prints a dataframe to stdout. df.toString() will also work but has no options .*/
fun DataFrame.print(colNames: Boolean = true, sep: String = "\t") = println(asString(colNames, sep))


fun DataFrame.asString(colNames: Boolean = true, sep: String = "\t"): String {

    if (this !is SimpleDataFrame) {
        // todo add support for grouped data here
        throw UnsupportedOperationException()
    }

    val sb = StringBuilder()

    if (colNames) this.cols.map { it.name }.joinToString(sep).apply { sb.appendln(this) }

    (1..nrow).map { row(it - 1).values.joinToString(sep).apply { sb.appendln(this) } }

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

    var df = this.ungroup() as SimpleDataFrame
//    if (this is GroupedDataFrame) {
//        df = df.ungroup()
//    }


    // create type
    val dataSpec = df.cols.map { "val ${it.name}: ${getScalarColType(it) }" }.joinToString(", ")
    println("data class ${dataClassName}(${dataSpec})")

    // map dataframe to
    // example: val dfEntries = df.rows.map {row ->  Df(row.get("first_name") as String ) }

    val attrMapping = df.cols.map { """ row["${it.name}"] as ${getScalarColType(it)}""" }.joinToString(", ")

    println("val ${dfVarName}Entries = ${dfVarName}.rows.map { row -> ${dataClassName}(${attrMapping}) }")
}

