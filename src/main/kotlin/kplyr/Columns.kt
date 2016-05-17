package kplyr

import kotlin.comparisons.nullsLast

/**
 */

// todo internalize these bits in here as much as possible

abstract class DataCol(val name: String) {

    open infix operator fun plus(something: Any): DataCol = throw UnsupportedOperationException()

    fun colHash(): IntArray = values().
            // convert nulls to odd number for hashing
            map({ it ?: Int.MIN_VALUE }).
            // hash
            map { it.hashCode() }.toIntArray()

    internal abstract fun values(): List<*>

    abstract val length: Int

    override fun toString(): String {
        return "$name [${getScalarColType(this)}]"
    }

}

internal val TMP_COLUMN = "___tmp"


// https://kotlinlang.org/api/latest/jvm/stdlib/kotlin/-double-array/index.html
// most methods are implemented it in kotlin.collections.plus
class DoubleCol(name: String, val values: List<Double?>) : DataCol(name) {
    override fun values(): List<*> = values

    override val length = values.size

    //        return values + values //wrong because it concatenates list and does not plus them
    override fun plus(something: Any): DataCol = when (something) {
        is DoubleCol -> Array(values.size, { values[it] }).toList()
                .apply { mapIndexed { index, rowVal -> naAwarePlus(rowVal, something.values[index]) } }

        is Number -> Array(values.size, { naAwarePlus(values[it], something.toDouble()) }).toList()

        else -> throw UnsupportedOperationException()
    }.let { DoubleCol(TMP_COLUMN, it) }

    private fun naAwarePlus(first: Double?, second: Double?): Double? {
        return if (first == null || second == null) null else first + second
    }
}

class IntCol(name: String, val values: List<Int?>) : DataCol(name) {
    override fun values(): List<*> = values

    override val length = values.size

    override fun plus(something: Any): DataCol = when (something) {
        is IntCol -> Array(values.size, { values[it] }).toList()
                .apply { mapIndexed { index, rowVal -> naAwarePlus(rowVal, something.values[index]) } }

        is Number -> Array(values.size, { naAwarePlus(values[it], something.toInt()) }).toList()
        else -> throw UnsupportedOperationException()
    }.let { IntCol(TMP_COLUMN, it) }


    private fun naAwarePlus(first: Int?, second: Int?): Int? {
        return if (first == null || second == null) null else first + second
    }
}

class AnyCol<T>(name: String, val values: List<T?>) : DataCol(name) {
    override fun values(): List<T?> = values

    override val length = values.size

    override fun plus(something: Any): DataCol = throw UnsupportedOperationException()

    private fun naAwarePlus(first: Int?, second: Int?): Int? {
        return if (first == null || second == null) null else first + second
    }
}

class BooleanCol(name: String, val values: List<Boolean?>) : DataCol(name) {
    override fun values(): List<*> = values

    override val length = values.size
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

// scalar operations
// remove because we must work with lists here
//infix operator fun DoubleArray.plus(i: Int): DoubleArray = map { it + i }.toDoubleArray()
fun List<Number>.mean(): Double = map { it.toDouble() }.sum() / size


//
// Vectorized boolean operators for DataColumn
//

infix fun List<Boolean>.AND(other: List<Boolean>): List<Boolean> = mapIndexed { index, first -> first && other[index] }.toList<Boolean>()

infix fun List<Boolean>.OR(other: List<Boolean>): List<Boolean> = mapIndexed { index, first -> first || other[index] }.toList<Boolean>()
infix fun List<Boolean>.XOR(other: List<Boolean>): List<Boolean> = mapIndexed { index, first -> first == other[index] }.toList<Boolean>()

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

fun <T> List<T?>.rmNA(expr: T.() -> Any?): List<Any?> = map { if (it != null) expr(it) else null }.toList()


//
// Arithmetic Utilities
//

// todo add NA argument

fun DataCol.min(): Double = when (this) {
    is DoubleCol -> values.filterNotNull().min()!!
    is IntCol -> values.filterNotNull().min()!!.toDouble()
    else -> throw UnsupportedOperationException()
}

fun DataCol.max(): Double = when (this) {
    is DoubleCol -> values.filterNotNull().max()!!
    is IntCol -> values.filterNotNull().max()!!.toDouble()
    else -> throw UnsupportedOperationException()
}

fun DataCol.mean(remNA: Boolean = false): Double = when (this) {
    is DoubleCol -> values.filterNotNull().mean()
    is IntCol -> values.filterNotNull().mean()
    else -> throw UnsupportedOperationException()
}


fun DataCol.median(remNA: Boolean = false): Double? = throw UnsupportedOperationException()
