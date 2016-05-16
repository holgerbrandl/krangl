package kutils.kplyr

/**
 * Created by brandl on 5/16/16.
 */
/**
Releated projects
----------------

https://github.com/mikera/vectorz
 * pandas cheat sheet: https://drive.google.com/folderview?id=0ByIrJAE4KMTtaGhRcXkxNHhmY2M&usp=sharing

 */

//abstract class DataCol(name:String, map: List<DataCell>) {}
abstract class DataCol(val name: String) {
//    fun setName(name: String) = {
//        this.name = name
//    }

    abstract infix operator fun plus(dataCol: DataCol): Any


    abstract fun colHash(): IntArray

    abstract val length: Int

//    abstract fun colData() : Any
}

// https://kotlinlang.org/api/latest/jvm/stdlib/kotlin/-double-array/index.html
// most methods are implemented it in kotlin.collections.plus
class DoubleCol(name: String, val values: DoubleArray) : DataCol(name) {

    override val length = values.size

    override fun colHash(): IntArray = values.map { it.hashCode() }.toIntArray()

    //        return values + values //wrong because it concatenates list and does not plus them
    override fun plus(dataCol: DataCol): DoubleArray = when (dataCol) {
        is DoubleCol -> DoubleArray(values.size, { values[it] }).
                apply { mapIndexed { index, rowVal -> rowVal + dataCol.values[index] } }
        else -> throw UnsupportedOperationException()
    }

//    constructor(name:String, val values: Array<Float>) : this(name, DoubleArray())
}

class IntCol(name: String, val values: IntArray) : DataCol(name) {

    override val length = values.size

    override fun colHash(): IntArray = values.map { it.hashCode() }.toIntArray()

    //        return values + values //wrong because it concatenates list and does not plus them
    override fun plus(dataCol: DataCol): IntArray = when (dataCol) {
        is IntCol -> IntArray(values.size, { values[it] }).
                apply { mapIndexed { index, rowVal -> rowVal + dataCol.values[index] } }
        else -> throw UnsupportedOperationException()
    }

//    constructor(name:String, val values: Array<Float>) : this(name, IntArray())
}

class BooleanCol(name: String, val values: BooleanArray) : DataCol(name) {


    override val length = values.size


    override fun plus(dataCol: DataCol): BooleanArray {
        // todo maybe plus should throw an excption for boolean columns instead of doing a counterintuitive AND??

        //        return values + values //wrong because it concatenates list and does not plus them

        return when (dataCol) {
            is BooleanCol -> BooleanArray(values.size, { values[it] }).
                    apply { mapIndexed { index, rowVal -> rowVal && dataCol.values[index] } }
            else -> throw UnsupportedOperationException()
        }
    }

    override fun colHash(): IntArray = values.map { it.hashCode() }.toIntArray()
//    constructor(name:String, val values: Array<Float>) : this(name, BooleanArray())
}

class StringCol(name: String, val values: List<String>) : DataCol(name) {

    override val length = values.size
//    override fun colData() = values

    override fun colHash(): IntArray = values.map { it.hashCode() }.toIntArray()
//    constructor(name:String, val values: Array<Float>) : this(name, DoubleArray())

    override fun plus(dataCol: DataCol): List<String> {
        if (dataCol is StringCol) {
            return values.zip(dataCol.values).map { it.first + it.second }
        } else {
            throw IllegalArgumentException("non-numeric argument to binary operator")
        }
    }
}

// scalar operations
infix operator fun DoubleArray.plus(i: Int): DoubleArray = map { it + i }.toDoubleArray()

infix operator fun DoubleArray.minus(i: Int): DoubleArray = map { it + i }.toDoubleArray()


// vectorized boolean operators
infix fun BooleanArray.AND(other: BooleanArray): BooleanArray = mapIndexed { index, first -> first && other[index] }.toBooleanArray()

infix fun BooleanArray.OR(other: BooleanArray): BooleanArray = mapIndexed { index, first -> first || other[index] }.toBooleanArray()
infix fun BooleanArray.XOR(other: BooleanArray): BooleanArray = mapIndexed { index, first -> first == other[index] }.toBooleanArray()

infix fun DataCol.gt(i: Int): BooleanArray = when (this) {
    is DoubleCol -> this.values.map({ it > i }).toBooleanArray()
    is IntCol -> this.values.map({ it > i }).toBooleanArray()
    else -> throw UnsupportedOperationException()
}

infix fun DataCol.lt(i: Int): BooleanArray = when (this) {
    is DoubleCol -> this.values.map({ it < i }).toBooleanArray()
    is IntCol -> this.values.map({ it < i }).toBooleanArray()
    else -> throw UnsupportedOperationException()
}

infix fun DataCol.eq(i: Any): BooleanArray = when (this) {
    is DoubleCol -> this.values.map({ it == i }).toBooleanArray()
    is IntCol -> this.values.map({ it == i }).toBooleanArray()
    is StringCol -> this.values.map({ it == i }).toBooleanArray()
    else -> throw UnsupportedOperationException()
}


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

