package kplyr

import mean
import median
import kotlin.comparisons.nullsLast


// todo internalize these bits in here as much as possible

abstract class DataCol(val name: String) {

    open infix operator fun plus(something: Any): DataCol = throw UnsupportedOperationException()
    open infix operator fun minus(something: Any): DataCol = throw UnsupportedOperationException()
    open infix operator fun div(something: Any): DataCol = throw UnsupportedOperationException()
    open infix operator fun times(something: Any): DataCol = throw UnsupportedOperationException()

    internal abstract fun values(): List<*>

    abstract val length: Int

    override fun toString(): String {
        return "$name [${getScalarColType(this)}]"
    }
}


private fun <T> naAwareOp(first: T?, second: T?, op: (T, T) -> T): T? {
    return if (first == null || second == null) null else op(first, second)
}


internal fun getScalarColType(it: DataCol): String = it.javaClass.simpleName.removeSuffix("Col")
//        when (it) {
//    is DoubleCol -> "Double"
//    is IntCol -> "Int"
//    is BooleanCol -> "Boolean"
//    is StringCol -> "String"
//    else -> throw  UnsupportedOperationException()
//}

internal val TMP_COLUMN = "___tmp"


// https://kotlinlang.org/api/latest/jvm/stdlib/kotlin/-double-array/index.html
// most methods are implemented it in kotlin.collections.plus
class DoubleCol(name: String, val values: List<Double?>) : DataCol(name) {

    override fun values(): List<*> = values

    override val length = values.size

    override fun plus(something: Any): DataCol = arithOp(something, { a, b -> a + b })
    override fun minus(something: Any): DataCol = arithOp(something, { a, b -> a - b })

    override fun times(something: Any): DataCol = arithOp(something, { a, b -> a * b })
    override fun div(something: Any): DataCol = arithOp(something, { a, b -> a / b })


    private fun arithOp(something: Any, op: (Double, Double) -> Double): DataCol = when (something) {
        is DoubleCol -> Array(values.size, { values[it] }).toList()
                .apply { mapIndexed { index, rowVal -> naAwareOp(rowVal, something.values[index], op) } }

        is Number -> Array(values.size, { naAwareOp(values[it], something.toDouble(), op) }).toList()
        else -> throw UnsupportedOperationException()
    }.let { DoubleCol(TMP_COLUMN, it) }
}


class IntCol(name: String, val values: List<Int?>) : DataCol(name) {

    override fun values(): List<*> = values

    override val length = values.size

    override fun plus(something: Any): DataCol = arithOp(something, { a, b -> a + b })
    override fun minus(something: Any): DataCol = arithOp(something, { a, b -> a * b })

    override fun times(something: Any): DataCol = arithOp(something, { a, b -> a - b })
    override fun div(something: Any): DataCol = arithOp(something, { a, b -> Math.round(a.toDouble() / b.toDouble()).toInt() })


    private fun arithOp(something: Any, op: (Int, Int) -> Int): DataCol = when (something) {
        is IntCol -> Array(values.size, { values[it] }).toList()
                .apply { mapIndexed { index, rowVal -> naAwareOp(rowVal, something.values[index], op) } }

        is Number -> Array(values.size, { naAwareOp(values[it], something.toInt(), op) }).toList()
        else -> throw UnsupportedOperationException()
    }.let { IntCol(TMP_COLUMN, it) }
}


class StringCol(name: String, val values: List<String?>) : DataCol(name) {

    override fun values(): List<*> = values

    override val length = values.size


    override fun plus(something: Any): DataCol = when (something) {
        is StringCol -> values.zip(something.values).map { naAwarePlus(it.first, it.second) }
        else -> values.map { naAwarePlus(it, something.toString()) }
    }.let { StringCol(TMP_COLUMN, it) }

    private fun naAwarePlus(first: String?, second: String?): String? {
        return if (first == null || second == null) null else first + second
    }
}

class AnyCol<T>(name: String, val values: List<T?>) : DataCol(name) {

    override fun values(): List<T?> = values

    override val length = values.size
}


class BooleanCol(name: String, val values: List<Boolean?>) : DataCol(name) {
    override fun values(): List<*> = values

    override val length = values.size
}


//
// Vectorized boolean operators for DataColumn
//

infix fun List<Boolean>.AND(other: List<Boolean>): List<Boolean> = mapIndexed { index, first -> first && other[index] }
infix fun List<Boolean>.OR(other: List<Boolean>): List<Boolean> = mapIndexed { index, first -> first || other[index] }
infix fun List<Boolean>.XOR(other: List<Boolean>): List<Boolean> = mapIndexed { index, first -> first == other[index] }


infix fun DataCol.gt(i: Number) = when (this) {
    is DoubleCol -> this.values.map { nullsLast<Double>().compare(it, i.toDouble()) > 0 }
    is IntCol -> this.values.map { nullsLast<Double>().compare(it!!.toDouble(), i.toDouble()) > 0 }
    else -> throw UnsupportedOperationException()
}.toBooleanArray()

infix fun DataCol.lt(i: Int) = gt(i).map { !it }.toBooleanArray()

infix fun DataCol.eq(i: Any): BooleanArray = when (this) {
    is DoubleCol -> this.values().map({ it == i })
    is IntCol -> this.values.map({ it == i })
    is BooleanCol -> this.values.map({ it == i })
    is StringCol -> this.values.map({ it == i })
    else -> throw UnsupportedOperationException()
}.toBooleanArray()


// convenience getters for column data

fun DataCol.asStrings(): List<String?> = this.values() as List<String?>
fun DataCol.asInts(): List<Int?> = this.values() as List<Int?>
fun DataCol.asDoubles(): List<Double?> = this.values() as List<Double?>
fun DataCol.asBooleans(): List<Boolean?> = this.values() as List<Boolean?>


/**
 * Non-null helper here for vectorized column operations. Allows to work with non-NA values but keeps them in resulting vector
 * Helpful for non standard column types.
 */
fun <T> DataCol.data(): List<T?> = this.values() as List<T>

/** Allows to transform column data into list of same length ignoring missing values, which are kept but processing
 * can be done in a non-null manner.
 */
fun <T> DataCol.dataNA(expr: T.() -> Any?): List<Any?> = (this.values() as List<T>).map { if (it != null) expr(it) else null }.toList()

fun <T> List<T?>.ignoreNA(expr: T.() -> Any?): List<Any?> = map { if (it != null) expr(it) else null }.toList()


//
// Arithmetic Utilities
//

fun DataCol.min(removeNA: Boolean = false): Double? = when (this) {
    is DoubleCol -> if (removeNA) values.filterNotNull().min() else values.map { it!! }.min()
    is IntCol -> if (removeNA) values.filterNotNull().min()?.toDouble() else values.map { it!! }.min()?.toDouble()
    else -> throw UnsupportedOperationException()
}

fun DataCol.max(removeNA: Boolean = false): Double? = when (this) {
    is DoubleCol -> if (removeNA) values.filterNotNull().max() else values.map { it!! }.max()
    is IntCol -> if (removeNA) values.filterNotNull().max()?.toDouble() else values.map { it!! }.max()?.toDouble()
    else -> throw UnsupportedOperationException()
}

fun DataCol.mean(removeNA: Boolean = false): Double? = when (this) {
    is DoubleCol -> if (removeNA) values.filterNotNull().mean() else values.map { it!! }.mean()
    is IntCol -> if (removeNA) values.filterNotNull().mean() else values.map { it!! }.mean()
    else -> throw UnsupportedOperationException()
}

fun DataCol.median(removeNA: Boolean = false): Double? = when (this) {
    is DoubleCol -> if (removeNA) values.filterNotNull().median() else values.map { it!! }.median()
    is IntCol -> if (removeNA) values.filterNotNull().map { it.toDouble() }.median() else values.map { it!!.toDouble() }.median()
    else -> throw UnsupportedOperationException()
}


//
// Category/String helper extensions
//

// see https://github.com/holgerbrandl/kplyr/issues/3