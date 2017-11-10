package krangl

/** Extension API to allow for column subsetting (both positive and negative)*/

/**
 * @author Holger Brandl
 */


//
// Public API
//



//https://kotlinlang.org/docs/reference/inline-functions.html#reified-type-parameters
/** Select columns by column type */
inline fun <reified T : DataCol> DataFrame.select() = select(cols.filter { it is T }.map { it.name })

/** Remove columns by column type */
inline fun <reified T : DataCol> DataFrame.remove() = select(cols.filter { !(it is T) }.map { it.name })
//inline fun <reified T : DataCol> DataFrame.select() = select(cols.filter{ it is T }.map{it.name})


class InvalidColumnSelectException(msg: String) : RuntimeException(msg)

class ColNames(val names: List<String>)

typealias ColumnSelector = ColNames.() -> List<Boolean?>

//
// select utilities inspired by see http://www.rdocumentation.org/packages/dplyr/functions/select
//

fun ColNames.matches(regex: String) = names.map { it.matches(regex.toRegex()) }

fun ColNames.startsWith(prefix: String) = names.map { it.startsWith(prefix) }.falseAsNull()
fun ColNames.endsWith(prefix: String) = names.map { it.endsWith(prefix) }.falseAsNull()
fun ColNames.matches(regex: Regex) = names.map { it.matches(regex) }.falseAsNull()

fun ColNames.oneOf(vararg someNames: String): List<Boolean?> = names.map { someNames.contains(it) }.falseAsNull()
fun ColNames.oneOf(someNames: List<String>): List<Boolean?> = names.map { someNames.contains(it) }.falseAsNull()
fun ColNames.all() = Array(names.size, { true }).toList() // unclear purpose

fun ColNames.range(from: String, to: String): List<Boolean?> {
    val rangeStart = names.indexOf(from)
    val rangeEnd = names.indexOf(to)

    val rangeSelection = (rangeStart..rangeEnd).map { names[it] }
    return names.map { rangeSelection.contains(it) }.falseAsNull()
}


fun ColumnSelector.unaryNot(): ColumnSelector = fun ColNames.(): List<Boolean?> = this.this@unaryNot().map{ it?.not() }


// normally, there should be no need for them. We just do positive selection and either use renmove or select
// BUT: verbs like gather still need to support negative selection
fun ColNames.except(vararg columns: String) = names.map { !columns.contains(it) }.trueAsNull()
fun ColNames.except(columnSelector: ColumnSelector) = columnSelector(this).not()




// commented out because it's not clear how to use it
//val foo: ColumnSelector = { startsWith("foo")
//sleepData.select(foo AND { endsWith("dfd") })
//infix fun ColumnSelector.AND(other: ColumnSelector): ColumnSelector = fun ColNames.(): List<Boolean?> {
//    return this.this@AND().zip(this.other()).map { nullAwareAnd(it.first, it.second) }
//}


//@Deprecated("will be removed since this affects String namespace it might be not a good idea")
//operator fun String.unaryMinus() = fun ColNames.(): List<Boolean?> = names.map { it != this@unaryMinus }.trueAsNull()
//operator fun Iterable<String>.unaryMinus() = fun ColNames.(): List<Boolean> = names.map { !this@unaryMinus.contains(it) }
//operator fun List<Boolean?>.unaryMinus() = not()

operator fun List<Boolean?>.not() = map { it?.not() }


//
// Internal API
//


internal fun DataFrame.reduceColSelectors(which: Array<out ColumnSelector>): List<Boolean?> =
        which.map { it(ColNames(names)) }.reduce { selA, selB -> selA nullAwareAND selB }


internal infix fun List<Boolean?>.nullAwareAND(other: List<Boolean?>): List<Boolean?> = this.zip(other).map {
    nullAwareAnd(it.first, it.second)
}


internal fun List<Boolean>.falseAsNull() = map { if (!it) null else true }
internal fun List<Boolean>.trueAsNull() = map { if (it) null else false }
internal fun List<Boolean?>.nullAsFalse(): List<Boolean> = map { it ?: false }



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

