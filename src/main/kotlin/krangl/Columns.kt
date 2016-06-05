package krangl

import kotlin.comparisons.nullsLast


// todo internalize these bits in here as much as possible

abstract class DataCol(val name: String) {

    open infix operator fun plus(something: Any): DataCol = throw UnsupportedOperationException()
    open infix operator fun minus(something: Any): DataCol = throw UnsupportedOperationException()
    open infix operator fun div(something: Any): DataCol = throw UnsupportedOperationException()
    open infix operator fun times(something: Any): DataCol = throw UnsupportedOperationException()

    internal abstract fun values(): Array<*>

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
class DoubleCol(name: String, val values: Array<Double?>) : DataCol(name) {

    constructor(name: String, values: List<Double?>) : this(name, values.toTypedArray())

    override fun values(): Array<Double?> = values

    override val length = values.size

    override fun plus(something: Any): DataCol = arithOp(something, { a, b -> a + b })
    override fun minus(something: Any): DataCol = arithOp(something, { a, b -> a - b })

    override fun times(something: Any): DataCol = arithOp(something, { a, b -> a * b })
    override fun div(something: Any): DataCol = arithOp(something, { a, b -> a / b })


    private fun arithOp(something: Any, op: (Double, Double) -> Double): DataCol = when (something) {
        is DoubleCol -> Array(values.size, { values[it] })
                .apply { mapIndexed { index, rowVal -> naAwareOp(rowVal, something.values[index], op) } }

        is Number -> Array(values.size, { naAwareOp(values[it], something.toDouble(), op) })
        else -> throw UnsupportedOperationException()
    }.let { DoubleCol(TMP_COLUMN, it) }
}


class IntCol(name: String, val values: Array<Int?>) : DataCol(name) {

    constructor(name: String, values: List<Int?>) : this(name, values.toTypedArray())

    override fun values(): Array<Int?> = values

    override val length = values.size

    override fun plus(something: Any): DataCol = arithOp(something, { a, b -> a + b })
    override fun minus(something: Any): DataCol = arithOp(something, { a, b -> a * b })

    override fun times(something: Any): DataCol = arithOp(something, { a, b -> a - b })
    override fun div(something: Any): DataCol = arithOp(something, { a, b -> Math.round(a.toDouble() / b.toDouble()).toInt() })


    private fun arithOp(something: Any, op: (Int, Int) -> Int): DataCol = when (something) {
        is IntCol -> Array(values.size, { values[it] })
                .apply { mapIndexed { index, rowVal -> naAwareOp(rowVal, something.values[index], op) } }

        is Number -> Array(values.size, { naAwareOp(values[it], something.toInt(), op) })
        else -> throw UnsupportedOperationException()
    }.let { IntCol(TMP_COLUMN, it) }
}


class StringCol(name: String, val values: Array<String?>) : DataCol(name) {

    constructor(name: String, values: List<String?>) : this(name, values.toTypedArray())

    override fun values(): Array<String?> = values

    override val length = values.size


    override fun plus(something: Any): DataCol = when (something) {
        is StringCol -> values.zip(something.values).map { naAwarePlus(it.first, it.second) }.toTypedArray()
        else -> values.map { naAwarePlus(it, something.toString()) }.toTypedArray()
    }.let { StringCol(TMP_COLUMN, it) }

    private fun naAwarePlus(first: String?, second: String?): String? {
        return if (first == null || second == null) null else first + second
    }
}

class AnyCol(name: String, val values: Array<Any?>) : DataCol(name) {
    constructor(name: String, values: List<Any?>) : this(name, values.toTypedArray<Any?>())

    override fun values(): Array<Any?> = values

    override val length = values.size
}


class BooleanCol(name: String, val values: Array<Boolean?>) : DataCol(name) {
    constructor(name: String, values: List<Boolean?>) : this(name, values.toTypedArray())

    override fun values(): Array<Boolean?> = values

    override val length = values.size
}


//
// Vectorized boolean operators for DataColumn
//

infix fun List<Boolean>.AND(other: List<Boolean>): List<Boolean> = mapIndexed { index, first -> first && other[index] }
infix fun List<Boolean>.OR(other: List<Boolean>): List<Boolean> = mapIndexed { index, first -> first || other[index] }
infix fun List<Boolean>.XOR(other: List<Boolean>): List<Boolean> = mapIndexed { index, first -> first == other[index] }

// Boolean operators for filter expressions
infix fun BooleanArray.AND(other: BooleanArray) = mapIndexed { index, first -> first && other[index] }.toList()

infix fun BooleanArray.OR(other: BooleanArray) = mapIndexed { index, first -> first || other[index] }.toBooleanArray()


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

fun DataCol.asStrings(): Array<String?> = (this as StringCol).values()
fun DataCol.asInts(): Array<Int?> = (this as IntCol).values()
fun DataCol.asDoubles(): Array<Double?> = (this as DoubleCol).values()
fun DataCol.asBooleans(): Array<Boolean?> = (this as BooleanCol).values()


/**
 * Non-null helper here for vectorized column operations. Allows to work with non-NA values but keeps them in resulting vector
 * Helpful for non standard column types.
 */
fun <T> DataCol.data(): List<T?> = this.values() as List<T>

fun DataCol.isNA(): BooleanArray = this.values().map { it == null }.toBooleanArray()


/** Allows to transform column data into list of same length ignoring missing values, which are kept but processing
 * can be done in a non-null manner.
 */
fun <T> DataCol.dataNA(expr: T.() -> Any?): List<Any?> = (this.values() as List<T>).map { if (it != null) expr(it) else null }.toList()

/** Allows to process a list of null-containing elements with an expression. NA will be kept where they were in the resulting table.*/
fun <T, R> Array<T?>.ignoreNA(expr: T.() -> R?): List<R?> = map { if (it != null) expr(it) else null }.toList()


//
// Arithmetic Utilities
//

//todo improve java interop by annotating with https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.jvm/-throws/


// this would work as well but we would gain little as long as we're just providing min, max, median, and mean. Others
// like quantile() already require additional parameters and the approach would no longer work

// //val fn : (List<Double>) -> Double = kotlin.collections.min
// //val fn : (List<Double>) -> Double = Math::min

//private fun DataCol.arithOp1(removeNA: Boolean, op: (List<Double>) -> Double): Double? = when (this) {
//    is DoubleCol -> values.run { if (removeNA) filterNotNull() else forceNotNull() }.let { op(it) }
//    is IntCol -> values.map { it?.toDouble() }.run { if (removeNA) filterNotNull() else forceNotNull() }.let { op(it) }
//    else -> throw InvalidColumnOperationException(this)
//}
//fun DataCol.min(removeNA: Boolean = false) = arithOp1(removeNA, kotlin.collections.min)


/**
 * Calculates the minimum of the column values.
 *
 * @param removeNA If `true` missing values will be excluded from the operation
 * @throws MissingValueException If removeNA is `false` but the data contains missing values.
 * @throws InvalidColumnOperationException If the type of the receiver column is not numeric
 */
fun DataCol.min(removeNA: Boolean = false): Double? = when (this) {
    is DoubleCol -> values.run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.min()
    is IntCol -> values.map { it?.toDouble() }.toTypedArray().run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.min()
    else -> throw InvalidColumnOperationException(this)
}

/**
 * Calculates the maximum of the column values.
 *
 * @param removeNA If `true` missing values will be excluded from the operation
 * @throws MissingValueException If removeNA is `false` but the data contains missing values.
 * @throws InvalidColumnOperationException If the type of the receiver column is not numeric
 */
fun DataCol.max(removeNA: Boolean = false): Double? = when (this) {
    is DoubleCol -> values.run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.max()
    is IntCol -> values.map { it?.toDouble() }.toTypedArray().run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.max()
    else -> throw InvalidColumnOperationException(this)
}

/**
 * Calculates the arithmetic mean of the column values.
 *
 * @param removeNA If `true` missing values will be excluded from the operation
 * @throws MissingValueException If removeNA is `false` but the data contains missing values.
 * @throws InvalidColumnOperationException If the type of the receiver column is not numeric
 */
fun DataCol.mean(removeNA: Boolean = false): Double? = when (this) {
    is DoubleCol -> values.run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.mean()
    is IntCol -> values.map { it?.toDouble() }.toTypedArray().run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.mean()
    else -> throw InvalidColumnOperationException(this)
}

/**
 * Calculates the median of the column values.
 *
 * @param removeNA If `true` missing values will be excluded from the operation
 * @throws MissingValueException If removeNA is `false` but the data contains missing values.
 * @throws InvalidColumnOperationException If the type of the receiver column is not numeric
 */
fun DataCol.median(removeNA: Boolean = false): Double? = when (this) {
    is DoubleCol -> values.run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.median()
    is IntCol -> values.map { it?.toDouble() }.toTypedArray().run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.median()
    else -> throw InvalidColumnOperationException(this)
}

private fun <E : Number> Array<E?>.forceDoubleNotNull() = try {
    map { it!!.toDouble() }
} catch(e: KotlinNullPointerException) {
    throw MissingValueException("Missing values in data. Consider to use removeNA argument or DataCol.ignoreNA()")
}

private inline fun <reified E> Array<E?>.forceNotNull(): Array<E> = try {
    map { it!! }.toTypedArray()
} catch(e: KotlinNullPointerException) {
    throw MissingValueException("Missing values in data. Consider to use removeNA argument or DataCol.ignoreNA()")
}


// todo just inherit for Throwble once https://github.com/kotlintest/kotlintest/issues/20 is fixed

/** Thrown if an operation is applied to a column that contains missing values. */
// todo do we really want this? Shouldn't it rather be NA (or add parameter to suppress Exception )
class MissingValueException(msg: String) : RuntimeException(msg)

class InvalidColumnOperationException(msg: String) : RuntimeException(msg) {
    constructor(receiver: Any) : this(receiver.javaClass.simpleName + " is not a supported by this operation ")
}

class NonScalarValueException(tf: TableFormula, result: Any) :
        RuntimeException("summarize() formula for '${tf.resultName}' did not evaluate into a scalar value but into a '${result}'")
//
// Category/String helper extensions
//

// see https://github.com/holgerbrandl/krangl/issues/3