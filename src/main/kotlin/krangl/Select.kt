package krangl


//
// DataFrame extensions
//

/**
 * @author Holger Brandl
 */

/** Select columns by predicate.

Example

```
foo.select{ it is IntCol }
foo.select{ it.name.startsWith("bar") }
```
 */
//fun DataFrame.select(colSelector: (DataCol) -> Boolean) = selectByName(cols.filter(colSelector).map{it.name})
//
//
///** Convenience wrapper around to work with varag <code>krangl.DataFrame.select</code> */
//fun DataFrame.selectByName(columns: List<String>): DataFrame = selectByName(*columns.toTypedArray())
//
///** Keeps only the variables that match any of the given expressions. E.g. use `startsWith("foo")` to select for columns staring with 'foo'.*/
//fun DataFrame.selectByName(which: ColNames.() -> List<Boolean?>): DataFrame = selectByName(*arrayOf(which))
//
//fun DataFrame.selectByName(vararg which: ColNames.() -> List<Boolean?>): DataFrame {
//    val reducedSelector = reduceColSelectors(which)
//
//    return select(reducedSelector)
//}



//
// select() support API
//



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
//operator fun List<Boolean?>.not() = map { it?.not() } // todo needed?

//val another = -"dsf"

internal fun DataFrame.reduceColSelectors(which: Array<out ColNames.() -> List<Boolean?>>): List<Boolean?> {
    val reducedSelector = which.map { it(ColNames(names)) }.reduce { selA, selB -> selA nullAwareAND selB }
    return reducedSelector
}

internal fun DataFrame.select(which: List<Boolean?>): DataFrame {
    val colSelection: List<String> = colSelectAsNames(which)

    return selectByName(colSelection)
}

//https://kotlinlang.org/docs/reference/inline-functions.html#reified-type-parameters
inline fun <reified T : DataCol> DataFrame.select() = selectByName(cols.filter { it is T }.map { it.name })
//inline fun <reified T : DataCol> DataFrame.select() = select(cols.filter{ it is T }.map{it.name})


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