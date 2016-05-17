package kplyr

import kotlin.comparisons.nullsLast

/**
Releated projects
----------------

https://github.com/mikera/vectorz
 * pandas cheat sheet: https://drive.google.com/folderview?id=0ByIrJAE4KMTtaGhRcXkxNHhmY2M&usp=sharing

 */

// todo internalize these bits in here as much as possible

abstract class DataCol(val name: String) {

    open infix operator fun plus(dataCol: DataCol): Any = throw UnsupportedOperationException()

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

// https://kotlinlang.org/api/latest/jvm/stdlib/kotlin/-double-array/index.html
// most methods are implemented it in kotlin.collections.plus
class DoubleCol(name: String, val values: List<Double?>) : DataCol(name) {
    override fun values(): List<*> = values

    override val length = values.size

    //        return values + values //wrong because it concatenates list and does not plus them
    override fun plus(dataCol: DataCol): List<Double?> = when (dataCol) {
        is DoubleCol -> Array(values.size, { values[it] }).toList().
                apply { mapIndexed { index, rowVal -> naAwarePlus(rowVal, dataCol.values[index]) } }
        else -> throw UnsupportedOperationException()
    }

    private fun naAwarePlus(first: Double?, second: Double?): Double? {
        return if (first == null || second == null) null else first + second
    }
}

class IntCol(name: String, val values: List<Int?>) : DataCol(name) {
    override fun values(): List<*> = values

    override val length = values.size

    override fun plus(dataCol: DataCol): List<Int?> = when (dataCol) {
        is IntCol -> Array(values.size, { values[it] }).
                apply { mapIndexed { index, rowVal -> naAwarePlus(rowVal, dataCol.values[index]) } }.toList()
        else -> throw UnsupportedOperationException()
    }

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


    override fun plus(dataCol: DataCol): List<String> {
        if (dataCol is StringCol) {
            return values.zip(dataCol.values).map { it.first + it.second }
        } else {
            throw IllegalArgumentException("non-numeric argument to binary operator")
        }
    }
}

// scalar operations
// remove because we must work with lists here
//infix operator fun DoubleArray.plus(i: Int): DoubleArray = map { it + i }.toDoubleArray()
fun DoubleArray.mean(): Double = sum() / size


// vectorized boolean operators
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
    is DoubleCol -> this.values.map({ it == i })
    is IntCol -> this.values.map({ it == i })
    is BooleanCol -> this.values.map({ it == i })
    is StringCol -> this.values.map({ it == i })
    else -> throw UnsupportedOperationException()
}.toBooleanArray()


// Arithmetic Utilities
fun DataCol.max(): Double = when (this) {
    is DoubleCol -> values.filterNotNull().max()!!
    is IntCol -> values.filterNotNull().max()!!.toDouble()
    else -> throw UnsupportedOperationException()
}

// todo implement remNA
fun DataCol.mean(remNA: Boolean = false): Double = when (this) {
    is DoubleCol -> values.filterNotNull().toDoubleArray().mean()
    is DoubleCol -> values.filterNotNull().toDoubleArray().mean()
    else -> throw UnsupportedOperationException()
}


fun DataCol.min(): Double = when (this) {
    is DoubleCol -> values.filterNotNull().min()!!
    is IntCol -> values.filterNotNull().min()!!.toDouble()
    else -> throw UnsupportedOperationException()
}

fun DataCol.median(remNA: Boolean = false): Double? = throw UnsupportedOperationException()


//infix operator fun DoubleArray.plus(elements: DoubleArray): DoubleArray {
//    DoubleArray
//    val thisSize = size
//    val arraySize = elements.size
//    val result = Arrays.copyOf(this, thisSize + arraySize)
//    System.arraycopy(elements, 0, result, thisSize, arraySize)
//    return result
//}
//class IntCol(vararg val values: Int) : Column
//class BoolCol(vararg val values: Boolean) : Column

