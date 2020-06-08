package krangl

/** Extension API to allow for column subsetting (both positive and negative)*/

/**
 * @author Holger Brandl
 */


//
// Public API
//


class InvalidColumnSelectException(val colNames: List<String>, val selection: List<Boolean?>) : RuntimeException() {
    override val message: String?
        get() {
            val collapsed = colNames.zip(selection).toMap().map { (name, selected) ->
                when (selected) {
                    true -> "+" + name
                    false -> "-" + name
                    else -> "<null>"
                }
            }.joinToString(",")

            return "Mixing positive and negative selection does not have meaningful semantics and is not supported:\n" + collapsed
        }

}


class ColNames(val names: List<String>)

typealias ColumnSelector = ColNames.() -> List<Boolean?>


internal fun ColumnSelector.validate(df: DataFrame) {
    val which = ColNames(df.names).(this)().toList()

    if (which.filterNotNull().distinct().size > 1) {
        throw InvalidColumnSelectException(df.names, which)
    }
}


fun ColNames.matches(regex: String) = matches(regex.toRegex())
fun ColNames.matches(regex: Regex) = names.map { it.matches(regex) }.falseAsNull()

fun ColNames.startsWith(prefix: String) = names.map { it.startsWith(prefix) }.falseAsNull()
fun ColNames.endsWith(prefix: String) = names.map { it.endsWith(prefix) }.falseAsNull()

fun ColNames.listOf(vararg someNames: String): List<Boolean?> = names.map { someNames.contains(it) }.falseAsNull()
fun ColNames.listOf(someNames: List<String>): List<Boolean?> = names.map { someNames.contains(it) }.falseAsNull()

fun ColNames.all() = Array(names.size, { true }).toList() // unclear purpose

fun ColNames.range(from: String, to: String): List<Boolean?> {
    val rangeStart = names.indexOf(from)
    val rangeEnd = names.indexOf(to)

    val rangeSelection = (rangeStart..rangeEnd).map { names[it] }
    return names.map { rangeSelection.contains(it) }.falseAsNull()
}


// normally, there should be no need for them. We just do positive selection and either use renmove or select
// BUT: verbs like gather still need to support negative selection
/** Performs a negative selection by selecting all columns except the listed ones. */
fun ColNames.except(vararg columns: String): List<Boolean?> {
    return if (columns.isEmpty()) {
        names.map { true }
    } else {
        names.map { !columns.contains(it) }.trueAsNull()
    }
}

fun ColNames.except(columnSelector: ColumnSelector) = !columnSelector(this)
//fun ColNames.not(columnSelector: ColumnSelector) = columnSelector(this).not()

operator fun List<Boolean?>.not() = map { it?.not() }

//https://kotlinlang.org/docs/reference/inline-functions.html#reified-type-parameters
/** Select columns by column type */
inline fun <reified T : DataCol> DataFrame.select() = select(cols.filter { it is T }.map { it.name })

// more generic users might not get the intention if T is not contrained to TableCol
//inline fun <reified T> DataFrame.select() = select(cols.filter {
//    when {
//        it is AnyCol -> it.values.firstOrNull() is T
//        else -> it is T
//    }
//}.map { it.name })

/** Remove columns by column type */
inline fun <reified T : DataCol> DataFrame.remove() = select(names.minus(select<T>().names))


/**
 * Push some columns to the right end of a data-frame.
 */
fun DataFrame.moveRight(vararg columnNames: String): DataFrame = select((names - columnNames) + columnNames.asList())

/**
 * Push some columns to the left end of a data-frame.
 */
fun DataFrame.moveLeft(vararg columnNames: String): DataFrame = select(columnNames.asList() + (names - columnNames))


// commented out because it's not clear how to use it
//val foo: ColumnSelector = { startsWith("foo")
//sleepData.select(foo AND { endsWith("dfd") })
//infix fun ColumnSelector.AND(other: ColumnSelector): ColumnSelector = fun ColNames.(): List<Boolean?> {
//    return this.this@AND().zip(this.other()).map { nullAwareAnd(it.first, it.second) }
//}

// commented out because it's not clear how to use it
//fun ColumnSelector.unaryNot(): ColumnSelector = fun ColNames.(): List<Boolean?> = this.this@unaryNot().map { it?.not() }


//@Deprecated("will be removed since this affects String namespace it might be not a good idea")
//operator fun String.unaryMinus() = fun ColNames.(): List<Boolean?> = names.map { it != this@unaryMinus }.trueAsNull()
//operator fun Iterable<String>.unaryMinus() = fun ColNames.(): List<Boolean> = names.map { !this@unaryMinus.contains(it) }
//operator fun List<Boolean?>.unaryMinus() = not()


//
// Internal API
//


internal infix fun List<Boolean?>.nullAwareAND(other: List<Boolean?>): List<Boolean?> = this.zip(other).map {
    nullAwareAnd(it.first, it.second)
}


internal fun List<Boolean>.falseAsNull() = map { if (!it) null else true }
internal fun List<Boolean>.trueAsNull() = map { if (it) null else false }
internal fun List<Boolean?>.nullAsFalse(): BooleanArray = map { it ?: false }.toBooleanArray()
internal fun List<Boolean?>.nullAsTrue(): BooleanArray = map { it ?: true }.toBooleanArray()


// todo the collapse logic does not seem right: why would would null && true be true?
internal fun nullAwareAnd(first: Boolean?, second: Boolean?): Boolean? {
    return if (first == null && second == null) {
        null
    } else if (first != null && second != null) {
        first && second
    } else {
        first ?: second
    }
}

internal fun nullAwareOr(first: Boolean?, second: Boolean?): Boolean? {
    return if (first == null && second == null) {
        null
    } else if (first != null && second != null) {
        first || second
    } else {
        first ?: second
    }
}

internal fun DataFrame.select(which: List<Boolean?>): DataFrame = select { which }

//private val <T> List<T>.isPositiveSelect: Boolean
//    get() =  any { it == true }
//private val <T> List<T>.isNegativeSelect: Boolean
//    get() =  !isPositiveSelect

internal fun DataFrame.reduceColSelectors(which: Array<out ColumnSelector>): ColumnSelector {
    // follow dplyr::select here, to:
    // If the first expression is negative, select() will automatically start with all variables.

    //    val extWhich = if(which.isNotEmpty() && which.first()(ColNames((names))).isNegativeSelect){
    //        val seedSelect : ColumnSelector = { all() }
    //        listOf(seedSelect).toMutableList().apply { addAll(which) }.toTypedArray()
    //    }else{
    //        which
    //    }

    return which
            .map { it(ColNames(names)) }
            .reduce { selA, selB -> selA nullAwareAND selB }
            .let { { it } }
}


internal fun DataFrame.colSelectAsNames(columnSelect: ColumnSelector): List<String> {
    columnSelect.validate(this)

    val which = ColNames(names).columnSelect()
    require(which.size == ncol) { "selector array has different dimension than data-frame" }

    // map boolean array to string selection
    val isPosSelection = which.count { it == true } > 0 || which.filterNotNull().isEmpty()
    val whichComplete = which.map { it ?: !isPosSelection }

    val colSelection: List<String> = names
            .zip(whichComplete)
            .filter { it.second }.map { it.first }

    return colSelection
}
