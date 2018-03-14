package krangl

import krangl.ArrayUtils.handleArrayErasure
import java.lang.UnsupportedOperationException
import java.util.*


// todo internalize these bits in here as much as possible

abstract class DataCol(val name: String) {


    open infix operator fun plus(something: Number): DataCol = plusInternal(something)
    open infix operator fun plus(something: DataCol): DataCol = plusInternal(something)
    open infix operator fun plus(something: Iterable<*>): DataCol = plusInternal(handleArrayErasure("foo", something.toList().toTypedArray()))
    open protected fun plusInternal(something: Any): DataCol = throw UnsupportedOperationException()


    open infix operator fun minus(something: Number): DataCol = minusInternal(something)
    open infix operator fun minus(something: DataCol): DataCol = minusInternal(something)
    open protected fun minusInternal(something: Any): DataCol = throw UnsupportedOperationException()

    open infix operator fun div(something: Number): DataCol = divInternal(something)
    open infix operator fun div(something: DataCol): DataCol = divInternal(something)
    open protected fun divInternal(something: Any): DataCol = throw UnsupportedOperationException()


    open infix operator fun times(something: Number): DataCol = timesInternal(something)
    open infix operator fun times(something: DataCol): DataCol = timesInternal(something)
    open protected infix fun timesInternal(something: Any): DataCol = throw UnsupportedOperationException()


    infix operator fun plus(something: String): DataCol = when (this) {
        is StringCol -> values.map { naAwarePlus(it, something) }
        else -> values().map { (it?.toString() ?: MISSING_VALUE) + something }
    }.toTypedArray().let { StringCol(TMP_COLUMN, it) }


    operator fun unaryMinus(): DataCol = this * -1

    open operator fun not(): DataCol = throw UnsupportedOperationException()

    abstract fun values(): Array<*>

    abstract val length: Int

    override fun toString(): String {
        return "$name [${getScalarColType(this)}]"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DataCol) return false

        if (name != other.name) return false
        if (length != other.length) return false
        //        http://stackoverflow.com/questions/35272761/how-to-compare-two-arrays-in-kotlin
        if (Arrays.equals(values(), other.values())) return false

        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + length + Arrays.hashCode(values())
        return result
    }

    operator fun get(index: Int) = values()[index]
}


private inline fun <T> naAwareOp(first: T?, second: T?, op: (T, T) -> T): T? {
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

internal fun wrappedNameIfNecessary(it: DataCol): String = it.name.run {
    if(this.contains(kotlin.text.Regex("\\s"))) {
        "`$this`"
    } else {
        this
    }
}

internal val TMP_COLUMN = "___tmp"


// https://kotlinlang.org/api/latest/jvm/stdlib/kotlin/-double-array/index.html
// most methods are implemented it in kotlin.collections.plus
class DoubleCol(name: String, val values: Array<Double?>) : DataCol(name) {

    constructor(name: String, values: List<Double?>) : this(name, values.toTypedArray())

    override fun values(): Array<Double?> = values

    override val length = values.size

    override fun plusInternal(something: Any): DataCol = arithOp(something, { a, b -> a + b })
    override fun minusInternal(something: Any): DataCol = arithOp(something, { a, b -> a - b })

    override fun timesInternal(something: Any): DataCol = arithOp(something, { a, b -> a * b })
    override fun divInternal(something: Any): DataCol = arithOp(something, { a, b -> a / b })


    private fun arithOp(something: Any, op: (Double, Double) -> Double): DataCol = when (something) {
        is DoubleCol -> Array(values.size) { it -> naAwareOp(this.values[it], something.values[it], op) }
        is IntCol -> Array(values.size) { it -> naAwareOp(this.values[it], something.values[it]?.toDouble(), op) }
        is Number -> Array(values.size, { naAwareOp(values[it], something.toDouble(), op) })
        else -> throw UnsupportedOperationException()
    }.let { DoubleCol(TMP_COLUMN, it) }
}


// todo what do we actually gain from having this type. It seems to be never used
abstract class NumberCol(name: String) : DataCol(name)


class IntCol(name: String, val values: Array<Int?>) : NumberCol(name) {

    constructor(name: String, values: List<Int?>) : this(name, values.toTypedArray())

    // does not work because of signature clash
    // constructor(name: String, vararg values: Int?) : this(name, values.asList().toTypedArray())

    override fun values(): Array<Int?> = values

    override val length = values.size


    override fun plusInternal(something: Any): DataCol = genericIntOp(something, { a, b -> a + b }) { a, b -> a + b }
    override fun minusInternal(something: Any): DataCol = genericIntOp(something, { a, b -> a - b }) { a, b -> a - b }
    override fun timesInternal(something: Any): DataCol = genericIntOp(something, { a, b -> a * b }, { a, b -> a * b })
    override fun divInternal(something: Any): DataCol = doubleOp(something, { a, b -> a / b })


    private fun genericIntOp(something: Any, intOp: (Int, Int) -> Int, doubleOp: (Double, Double) -> Double): DataCol {
        return when (something) {
            is IntCol -> intOp(something, intOp)
            is DoubleCol -> doubleOp(something, doubleOp)

            is Int -> this.intOp(something, intOp)
            is Double -> this.doubleOp(something, doubleOp)


            else -> throw UnsupportedOperationException()
        }
    }


    private fun doubleOp(something: Any, op: (Double, Double) -> Double): DataCol = when (something) {
        is DoubleCol -> Array(values.size) { it -> naAwareOp(this.values[it]?.toDouble(), something.values[it], op) }
        is Double -> Array(values.size, { naAwareOp(values[it]?.toDouble(), something, op) })

        else -> throw UnsupportedOperationException()
    }.let { handleArrayErasure(TMP_COLUMN, it) }


    private fun intOp(something: Any, op: (Int, Int) -> Int): DataCol = when (something) {
        is IntCol -> Array(values.size) { it -> naAwareOp(this.values[it], something.values[it], op) }
        is Int -> Array(values.size, { naAwareOp(values[it], something, op) })

        else -> throw UnsupportedOperationException()
    }.let { handleArrayErasure(TMP_COLUMN, it) }
}


class StringCol(name: String, val values: Array<String?>) : DataCol(name) {

    constructor(name: String, values: List<String?>) : this(name, values.toTypedArray())

    override fun values(): Array<String?> = values

    override val length = values.size


    override fun plusInternal(something: Any): DataCol = when (something) {
        is DataCol -> Array(values.size, { values[it] }).mapIndexed { index, rowVal ->
            naAwarePlus(rowVal, something.values()[index]?.toString())
        }
        else -> throw UnsupportedOperationException()
    }.let {
        StringCol(TMP_COLUMN, it)
    }

    internal fun naAwarePlus(first: String?, second: String?): String? {
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

    override fun not(): DataCol {
        return BooleanCol(name, values.map { it?.not() })
    }

    override fun values(): Array<Boolean?> = values

    override val length = values.size
}


//
// Vectorized operations on columns
//

//infix fun List<Boolean>.AND(other: List<Boolean>): List<Boolean> = mapIndexed { index, first -> first && other[index] }
//infix fun List<Boolean>.OR(other: List<Boolean>): List<Boolean> = mapIndexed { index, first -> first || other[index] }
//infix fun List<Boolean>.XOR(other: List<Boolean>): List<Boolean> = mapIndexed { index, first -> first == other[index] }
infix fun List<Boolean?>.AND(other: List<Boolean?>): List<Boolean?> = mapIndexed { index, first -> nullAwareAnd(first, other[index]) }

infix fun List<Boolean?>.OR(other: List<Boolean?>): List<Boolean?> = mapIndexed { index, first -> nullAwareOr(first, other[index]) }
infix fun List<Boolean?>.XOR(other: List<Boolean?>): List<Boolean> = mapIndexed { index, first -> first == other[index] }

// Boolean operators for filter expressions
infix fun BooleanArray.AND(other: BooleanArray) = mapIndexed { index, first -> first && other[index] }.toBooleanArray()

infix fun BooleanArray.OR(other: BooleanArray) = mapIndexed { index, first -> first || other[index] }.toBooleanArray()


infix fun DataCol.gt(i: Number) = greaterThan(i)
infix fun DataCol.ge(i: Number) = greaterEqualsThan(i)

fun DataCol.greaterThan(i: Number) = when (this) {
    is DoubleCol -> this.values.map { nullsLast<Double>().compare(it, i.toDouble()) > 0 }
    is IntCol -> this.values.map { nullsLast<Double>().compare(it!!.toDouble(), i.toDouble()) > 0 }
    else -> throw UnsupportedOperationException()
}.toBooleanArray()

fun DataCol.greaterEqualsThan(i: Number) = when (this) {
    is DoubleCol -> this.values.map { nullsLast<Double>().compare(it, i.toDouble()) >= 0 }
    is IntCol -> this.values.map { nullsLast<Double>().compare(it!!.toDouble(), i.toDouble()) >= 0 }
    else -> throw UnsupportedOperationException()
}.toBooleanArray()


infix fun DataCol.lt(i: Int) = gt(i).map { !it }.toBooleanArray()
fun DataCol.lesserThan(i: Int) = gt(i).map { !it }.toBooleanArray()

infix fun DataCol.isEqualTo(i: Any): BooleanArray = eq(i)

infix fun DataCol.eq(i: Any): BooleanArray = when (this) {
    is DoubleCol -> this.values().map({ it == i })
    is IntCol -> this.values.map({ it == i })
    is BooleanCol -> this.values.map({ it == i })
    is StringCol -> this.values.map({ it == i })
    else -> throw UnsupportedOperationException()
}.toBooleanArray()


// convenience getters for column data
//fun DataCol.asStrings(): Array<String?> = (this as StringCol).values()
//fun DataCol.asStrings(): Array<String?> = asType<String>()
//fun DataCol.asInts(): Array<Int?> = asType<Int>()
//fun DataCol.asDoubles(): Array<Double?> = asType<Double>()
//fun DataCol.asBooleans(): Array<Boolean?> = asType<Boolean>()
fun DataCol.asStrings(): Array<String?> = columnCast<StringCol>().values

fun DataCol.asDoubles(): Array<Double?> {
    return when {
        this is IntCol -> Array(values.size, { (this[it] as Int?)?.toDouble() })
        else -> columnCast<DoubleCol>().values
    }
}

fun DataCol.asBooleans(): Array<Boolean?> = columnCast<BooleanCol>().values
fun DataCol.asInts(): Array<Int?> = columnCast<IntCol>().values

//fun DataCol.s(): Array<String?> = asStrings()
//fun DataCol.d(): Array<Double?> = asDoubles()
//fun DataCol.b(): Array<Boolean?> = asBooleans()
//fun DataCol.i(): Array<Int?> = asInts()


class ColumnTypeCastException(msg: String) : RuntimeException(msg)


// no longer needed but kept as a reified example
internal inline fun <reified R> DataCol.columnCast(): R {
    return try {
        this as R
    } catch (e: ClassCastException) {
        val msg = "Could not cast column '${name}' of type '${this::class.simpleName}' to type '${R::class}'"
        throw ColumnTypeCastException(msg)
    }
}


// does not work because internal array is of type object
//inline fun <reified T> DataCol.asType() = (this as AnyCol).values as Array<T>
@Suppress("UNCHECKED_CAST")
inline fun <reified R> DataCol.asType(): Array<R?> {
    //        val data = (this as AnyCol).values
    //        return Array(data.size) { index -> data[index] as R }


    //     much faster since it avoid copying the array
    return try {
        when {
            this is StringCol -> this.values as Array<R?>
            this is DoubleCol -> values as Array<R?>
            this is BooleanCol -> values as Array<R?>
            this is IntCol -> values as Array<R?>
            this is AnyCol && values.firstOrNull() is R -> Array(values.size) { index -> values[index] as R }
            else -> throw RuntimeException()
        }
    } catch (e: ClassCastException) {
        val msg = "Could not cast column '${name}' of type '${this::class.simpleName}' to type '${R::class}'"
        throw ColumnTypeCastException(msg)
    }
}


/** Allows to transform column data into list of same length ignoring missing values, which are kept but processing
 * can be done in a non-null manner.
 */
inline fun <reified T> DataCol.map(noinline expr: (T) -> Any?): List<Any?> {
    val recast = asType<T?>()

    return recast.map { if (it != null) expr(it) else null }.toList()
}

fun <T> Array<T?>.ignoreNA(expr: T.() -> Any?): List<Any?> = map { if (it != null) expr(it) else null }


// should this be dropeed entirely?
//internal inline fun <reified T, R> DataCol.map2(noinline expr: (T) -> R?): List<R?> {
//    val recast = asType<T?>()
//
//    return recast.mapNonNull (expr)
//}


/**
 * Non-null helper here for vectorized column operations. Allows to work with non-NA values but keeps them in resulting vector
 * Helpful for non standard column types.
 */
//fun <T> DataCol.data(): List<T?> = this.values() as List<T>

/** Maps a column to true for the NA values and `false` otherwise. */
fun DataCol.isNA(): BooleanArray = this.values().map { it == null }.toBooleanArray()

fun DataCol.isNotNA(): BooleanArray = this.values().map { it != null }.toBooleanArray()



/** Allows to process a list of null-containing elements with an expression. NA will be kept where they were in the resulting table.*/
fun <T, R> Array<T?>.mapNonNull(expr: (T) -> R?): List<R?> {
    return map { if (it != null) expr(it) else null }.toList()
}

//inline fun <T, reified R> Array<T>.mapNonNullArr(expr: T.() -> R?): Array<R?> {
//    return Array<R?>(size, { expr(this[it])})
//}


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
} catch (e: KotlinNullPointerException) {
    throw MissingValueException("Missing values in data. Consider to use removeNA argument or DataCol.ignoreNA()")
}

private inline fun <reified E> Array<E?>.forceNotNull(): Array<E> = try {
    map { it!! }.toTypedArray()
} catch (e: KotlinNullPointerException) {
    throw MissingValueException("Missing values in data. Consider to use removeNA argument or DataCol.ignoreNA()")
}


// todo just inherit for Throwble once https://github.com/kotlintest/kotlintest/issues/20 is fixed

/** Thrown if an operation is applied to a column that contains missing values. */
// todo do we really want this? Shouldn't it rather be NA (or add parameter to suppress Exception )
class MissingValueException(msg: String) : RuntimeException(msg)

class InvalidColumnOperationException(msg: String) : RuntimeException(msg) {
    constructor(receiver: Any) : this(receiver.javaClass.simpleName + " is not a supported by this operation ")
}

class NonScalarValueException(tf: ColumnFormula, result: Any) :
    RuntimeException("summarize() expression for '${tf.name}' did not evaluate into a scalar value but into a '${result}'")
//
// Category/String helper extensions
//

// see https://github.com/holgerbrandl/krangl/issues/3