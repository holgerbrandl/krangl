package krangl

import krangl.ArrayUtils.handleArrayErasure
import krangl.util.joinToMaxLengthString
import java.util.*


abstract class DataCol(val name: String) {  // tbd why not: Iterable<Any> ??


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
    }.toTypedArray().let { StringCol(tempColumnName(), it) }


    operator fun unaryMinus(): DataCol = this * -1

    open operator fun not(): DataCol = throw UnsupportedOperationException()

    abstract fun values(): Array<*>

    val hasNulls by lazy { values().any { it == null } }

    abstract val length: Int

    override fun toString(): String {
        val prefix = "$name [${getColumnType(this)}][$length]: "
        val peek = values().take(255).asSequence()
            .joinToMaxLengthString(maxLength = PRINT_MAX_WIDTH - prefix.length, transform = createValuePrinter(PRINT_MAX_DIGITS))

        return prefix + peek
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DataCol) return false

        if (name != other.name) return false
        if (length != other.length) return false
        //        http://stackoverflow.com/questions/35272761/how-to-compare-two-arrays-in-kotlin
        if (!Arrays.equals(values(), other.values())) return false

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

internal fun tempColumnName() = "tmp_col_" + UUID.randomUUID()


// https://kotlinlang.org/api/latest/jvm/stdlib/kotlin/-double-array/index.html
// most methods are implemented it in kotlin.collections.plus
class DoubleCol(name: String, val values: Array<Double?>) : DataCol(name) {

    constructor(name: String, values: List<Double?>) : this(name, values.toTypedArray())

    @Suppress("UNCHECKED_CAST")
    constructor(name: String, values: DoubleArray) : this(name, values.toTypedArray() as Array<Double?>)

    override fun values(): Array<Double?> = values

    override val length = values.size

    override fun plusInternal(something: Any): DataCol = arithOp(something, { a, b -> a + b })
    override fun minusInternal(something: Any): DataCol = arithOp(something, { a, b -> a - b })

    override fun timesInternal(something: Any): DataCol = arithOp(something, { a, b -> a * b })
    override fun divInternal(something: Any): DataCol = arithOp(something, { a, b -> a / b })


    private fun arithOp(something: Any, op: (Double, Double) -> Double): DataCol = when (something) {
        is DoubleCol -> Array(values.size) { it -> naAwareOp(this.values[it], something.values[it], op) }
        is IntCol -> Array(values.size) { it -> naAwareOp(this.values[it], something.values[it]?.toDouble(), op) }
        is LongCol -> Array(values.size) { it -> naAwareOp(this.values[it], something.values[it]?.toDouble(), op) }
        is Number -> Array(values.size, { naAwareOp(values[it], something.toDouble(), op) })
        else -> throw UnsupportedOperationException()
    }.let { DoubleCol(tempColumnName(), it) }
}


// todo what do we actually gain from having this type. It seems to be never used
abstract class NumberCol(name: String) : DataCol(name)


// no na in pandas int columns because of http://pandas.pydata.org/pandas-docs/stable/gotchas.html#support-for-integer-na

class IntCol(name: String, val values: Array<Int?>) : NumberCol(name) {

    constructor(name: String, values: List<Int?>) : this(name, values.toTypedArray())

    @Suppress("UNCHECKED_CAST")
    constructor(name: String, values: IntArray) : this(name, values.toTypedArray() as Array<Int?>)

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
    }.let { handleArrayErasure(tempColumnName(), it) }


    private fun intOp(something: Any, op: (Int, Int) -> Int): DataCol = when (something) {
        is IntCol -> Array(values.size) { it -> naAwareOp(this.values[it], something.values[it], op) }
        is Int -> Array(values.size, { naAwareOp(values[it], something, op) })

        else -> throw UnsupportedOperationException()
    }.let { handleArrayErasure(tempColumnName(), it) }
}


class LongCol(name: String, val values: Array<Long?>) : NumberCol(name) {

    constructor(name: String, values: List<Long?>) : this(name, values.toTypedArray())

    @Suppress("UNCHECKED_CAST")
    constructor(name: String, values: LongArray) : this(name, values.toTypedArray() as Array<Long?>)

    // does not work because of signature clash
    // constructor(name: String, vararg values: Int?) : this(name, values.asList().toTypedArray())

    override fun values(): Array<Long?> = values

    override val length = values.size


    override fun plusInternal(something: Any): DataCol = genericLongOp(something, { a, b -> a + b }) { a, b -> a + b }
    override fun minusInternal(something: Any): DataCol = genericLongOp(something, { a, b -> a - b }) { a, b -> a - b }
    override fun timesInternal(something: Any): DataCol = genericLongOp(something, { a, b -> a * b }, { a, b -> a * b })
    override fun divInternal(something: Any): DataCol = doubleOp(something, { a, b -> a / b })


    private fun genericLongOp(something: Any, longOp: (Long, Long) -> Long, doubleOp: (Double, Double) -> Double): DataCol {
        return when (something) {
            is IntCol -> longOp(something, longOp)
            is LongCol -> longOp(something, longOp)
            is DoubleCol -> doubleOp(something, doubleOp)

            is Int -> longOp(something, longOp)
            is Long -> longOp(something, longOp)
            is Double -> this.doubleOp(something, doubleOp)


            else -> throw UnsupportedOperationException()
        }
    }


    private fun doubleOp(something: Any, op: (Double, Double) -> Double): DataCol = when (something) {
        is DoubleCol -> Array(values.size) { it -> naAwareOp(this.values[it]?.toDouble(), something.values[it], op) }
        is Double -> Array(values.size, { naAwareOp(values[it]?.toDouble(), something, op) })

        else -> throw UnsupportedOperationException()
    }.let { handleArrayErasure(tempColumnName(), it) }


    private fun longOp(something: Any, op: (Long, Long) -> Long): DataCol = when (something) {
        is LongCol -> Array(values.size) { it -> naAwareOp(this.values[it], something.values[it], op) }
        is Long -> Array(values.size, { naAwareOp(values[it], something, op) })
        is IntCol -> Array(values.size) { it -> naAwareOp(this.values[it], something.values[it]?.toLong(), op) }
        is Int -> Array(values.size, { naAwareOp(values[it], something.toLong(), op) })

        else -> throw UnsupportedOperationException()
    }.let { handleArrayErasure(tempColumnName(), it) }
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
        StringCol(tempColumnName(), it)
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

//for a discussion about operator overloading see https://discuss.kotlinlang.org/t/post-1-0-roadmap/1496/33

//infix fun List<Boolean>.AND(other: List<Boolean>): List<Boolean> = mapIndexed { index, first -> first && other[index] }
//infix fun List<Boolean>.OR(other: List<Boolean>): List<Boolean> = mapIndexed { index, first -> first || other[index] }
//infix fun List<Boolean>.XOR(other: List<Boolean>): List<Boolean> = mapIndexed { index, first -> first == other[index] }

infix fun List<Boolean?>.AND(other: List<Boolean?>) = mapIndexed { index, first -> nullAwareAnd(first, other[index]) }
infix fun List<Boolean?>.OR(other: List<Boolean?>) = mapIndexed { index, first -> nullAwareOr(first, other[index]) }.nullAsFalse()
infix fun List<Boolean?>.XOR(other: List<Boolean?>) = mapIndexed { index, first -> first == other[index] }.nullAsFalse()

// Boolean operators for filter expressions
infix fun BooleanArray.AND(other: BooleanArray) = mapIndexed { index, first -> first && other[index] }.toBooleanArray()

infix fun BooleanArray.OR(other: BooleanArray) = mapIndexed { index, first -> first || other[index] }.toBooleanArray()

operator fun BooleanArray.not() = BooleanArray(this.size, { !this[it] })


// comparisons against fixed values

// note: https://github.com/JetBrains/Exposed is using a simlar apprach with `greaterEquals` (see their readme examples)
infix fun DataCol.gt(i: Number) = greaterThan(i)
infix fun DataCol.ge(i: Number) = greaterEqualsThan(i)

infix fun DataCol.lt(i: Number) = lesserThan(i)
infix fun DataCol.le(i: Number) = lesserEquals(i)

fun DataCol.greaterThan(i: Number) = _greaterThan(i).nullAsFalse()
fun DataCol.greaterEqualsThan(i: Number) = _greaterEqualsThan(i).nullAsFalse()

fun DataCol.lesserThan(i: Number) = (!_greaterEqualsThan(i)).nullAsFalse() //AND isNotNA()
fun DataCol.lesserEquals(i: Number) = (!_greaterThan(i)).nullAsFalse() //AND isNotNA()

private val doubleComp = nullsFirst<Double>()
private val intComp = nullsFirst<Int>()
private val longComp = nullsFirst<Long>()

internal fun DataCol._greaterThan(i: Number) = when (this) {
    is DoubleCol -> this.values.mapNonNull { doubleComp.compare(it, i.toDouble()) > 0 }
    is IntCol -> this.values.mapNonNull { doubleComp.compare(it.toDouble(), i.toDouble()) > 0 }
    is LongCol -> this.values.mapNonNull { doubleComp.compare(it.toDouble(), i.toDouble()) > 0 }
    else -> throw UnsupportedOperationException()
}

internal fun DataCol._greaterEqualsThan(i: Number) = when (this) {
    is DoubleCol -> this.values.mapNonNull { doubleComp.compare(it, i.toDouble()) >= 0 }
    is IntCol -> this.values.mapNonNull { doubleComp.compare(it.toDouble(), i.toDouble()) >= 0 }
    is LongCol -> this.values.mapNonNull { doubleComp.compare(it.toDouble(), i.toDouble()) >= 0 }
    else -> throw UnsupportedOperationException()
}


// column comparison

infix fun DataCol.gt(i: DataCol) = greaterThan(i)
infix fun DataCol.ge(i: DataCol) = greaterEqualsThan(i)

infix fun DataCol.lt(i: DataCol) = lesserThan(i)
infix fun DataCol.le(i: DataCol) = lesserEquals(i)

fun DataCol.greaterThan(i: DataCol) = _greaterThan(i).nullAsFalse()
fun DataCol.greaterEqualsThan(i: DataCol) = _greaterEqualsThan(i).nullAsFalse()

fun DataCol.lesserThan(i: DataCol) = (!_greaterEqualsThan(i)).nullAsFalse() //AND isNotNA()
fun DataCol.lesserEquals(i: DataCol) = (!_greaterThan(i)).nullAsFalse() //AND isNotNA()


internal fun DataCol._greaterThan(i: DataCol) = when (this) {
    is DoubleCol, is IntCol, is LongCol -> values().zip(i.values()).map { (a, b) -> doubleComp.compare((a as? Number)?.toDouble(), (b as? Number)?.toDouble()) > 0 }
//    is IntCol -> values.zip(i.values()).map { (a, b) -> intComp.compare(a, (b as Number?)?.toInt()) > 0 }
    else -> throw UnsupportedOperationException()
}


internal fun DataCol._greaterEqualsThan(i: DataCol) = when (this) {
    is DoubleCol, is IntCol, is LongCol -> values().zip(i.values()).map { (a, b) -> doubleComp.compare((a as? Number)?.toDouble(), (b as? Number)?.toDouble()) >= 0 }
//    is IntCol -> values.zip(i.values()).map { (a, b) -> intComp.compare(a, (b as Number?)?.toInt()) > 0 }
    else -> throw UnsupportedOperationException()
}


infix fun DataCol.isEqualTo(i: Any): BooleanArray = eq(i)

infix fun DataCol.eq(i: Any): BooleanArray = when (this) {
    is DoubleCol -> this.values().map({ it == i })
    is IntCol -> this.values.map({ it == i })
    is LongCol -> this.values.map({ it == i })
    is BooleanCol -> this.values.map({ it == i })
    is StringCol -> this.values.map({ it == i })
    else -> throw UnsupportedOperationException()
}.toBooleanArray()


//
// convenience getters for column data
//


//fun DataCol.asStrings(): Array<String?> = (this as StringCol).values()
//fun DataCol.asStrings(): Array<String?> = asType<String>()
//fun DataCol.asInts(): Array<Int?> = asType<Int>()
//fun DataCol.asDoubles(): Array<Double?> = asType<Double>()
//fun DataCol.asBooleans(): Array<Boolean?> = asType<Boolean>()
fun DataCol.asStrings(): Array<String?> = columnCast<StringCol>().values

fun DataCol.asDoubles(): Array<Double?> {
    return when {
        this is IntCol -> Array(values.size, { (this[it] as Int?)?.toDouble() })
        this is LongCol -> Array(values.size, { (this[it] as Long?)?.toDouble() })
        else -> columnCast<DoubleCol>().values
    }
}

fun DataCol.asBooleans(): Array<Boolean?> = columnCast<BooleanCol>().values
fun DataCol.asInts(): Array<Int?> = columnCast<IntCol>().values
fun DataCol.asLongs(): Array<Long?> = columnCast<LongCol>().values

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
            this is LongCol -> values as Array<R?>
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
// todo maybe this should return DataCol or similar to allow for more flexible operator overloading (see #39)
inline fun <reified T> DataCol.map(noinline expr: (T) -> Any?): List<Any?> {
    val recast = asType<T?>()

    return recast.map { if (it != null) expr(it) else null }.toList()
}

/** Allows to process a list of null-containing elements with an expression. NA will be kept where they were in the resulting table.*/
fun <T, R> Array<T?>.mapNonNull(expr: (T) -> R?): List<R?> {
    return map { if (it != null) expr(it) else null }.toList()
}


//fun <T> Array<T?>.ignoreNA(expr: T.() -> Any?): List<Any?> = map { if (it != null) expr(it) else null }


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

fun ExpressionContext.isNA(columnName: String): BooleanArray = this[columnName].values().map { it == null }.toBooleanArray()

fun DataCol.isNotNA(): BooleanArray = this.values().map { it != null }.toBooleanArray()
fun ExpressionContext.isNotNA(columnName: String): BooleanArray = isNA(columnName)


//inline fun <T, reified R> Array<T>.mapNonNullArr(expr: T.() -> R?): Array<R?> {
//    return Array<R?>(size, { expr(this[it])})
//}


//
// Arithmetic Utilities
//

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
    is LongCol -> values.map { it?.toDouble() }.toTypedArray().run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.min()
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
    is LongCol -> values.map { it?.toDouble() }.toTypedArray().run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.max()
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
    is LongCol -> values.map { it?.toDouble() }.toTypedArray().run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.mean()
    else -> throw InvalidColumnOperationException(this)
}

/**
 * Calculates the arithmetic mean of the column values.
 *
 * @param removeNA If `true` missing values will be excluded from the operation
 * @throws MissingValueException If removeNA is `false` but the data contains missing values.
 * @throws InvalidColumnOperationException If the type of the receiver column is not numeric
 */
fun DataCol.sum(removeNA: Boolean = false): Number? = when (this) {
    is DoubleCol -> values.run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.sum()
    is IntCol -> values.run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.sum()
    is LongCol -> values.run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.sum()
    is BooleanCol -> values.run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.sumBy { if (it) 1 else 0 }
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
    is LongCol -> values.map { it?.toDouble() }.toTypedArray().run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.median()
    else -> throw InvalidColumnOperationException(this)
}

/**
 * Calculates the standard deviation of the column values.
 *
 * @param removeNA If `true` missing values will be excluded from the operation
 * @throws MissingValueException If removeNA is `false` but the data contains missing values.
 * @throws InvalidColumnOperationException If the type of the receiver column is not numeric
 */
fun DataCol.sd(removeNA: Boolean = false): Double? = when (this) {
    is DoubleCol -> values.run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.sd()
    is IntCol -> values.map { it?.toDouble() }.toTypedArray().run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.sd()
    is LongCol -> values.map { it?.toDouble() }.toTypedArray().run { if (removeNA) filterNotNull().toTypedArray() else forceNotNull() }.sd()
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


/** Thrown if an operation is applied to a column that contains missing values. */
// todo do we really want this? Shouldn't it rather be NA (or add parameter to suppress Exception )
class MissingValueException(msg: String) : Throwable(msg)


internal const val INTERNAL_ERROR_MSG =
    "This looks like an issue with krangl. Please submit an example reproducing the problem/usecase to https://github.com/holgerbrandl/krangl/issues"

internal const val PLEASE_SUBMIT_MSG = "Feel welcome to submit a ticket to https://github.com/holgerbrandl/krangl/issues"


class DuplicatedColumnNameException(val names: List<String>) : RuntimeException() {

    override val message: String?
        get() {
            val duplicatedNames = names.groupBy { it }.filter { it.value.size > 1 }.keys

            return with(duplicatedNames) {
                when {
                    size == 1 -> "'${duplicatedNames.joinToString()}' is already present in data-frame"
                    size > 1 -> "'${duplicatedNames.joinToString()}' are already present in the data-frame"
                    else -> INTERNAL_ERROR_MSG
                }
            }
        }
}


class InvalidColumnOperationException(msg: String) : RuntimeException(msg) {
    constructor(receiver: Any) : this(receiver.javaClass.simpleName + " is not a supported by this operation ")
}

class NonScalarValueException(tf: ColumnFormula, result: Any) :
    RuntimeException("summarize() expression for '${tf.name}' did not evaluate into a scalar value but into a '${result}'")


class InvalidSortingPredicateException(result: Any) :
    RuntimeException("Sorting literal did not evaluate into boolean array, but instead to '${result}.")


//
// Category/String helper extensions
//

/** Vectorized string concatenation. */
// similar to https://stackoverflow.com/questions/9958506/element-wise-string-concatenation-in-numpy
infix fun List<Any?>.concat(right: List<Any?>) = zip(right).map { it.first.toString() + " " + it.second.toString() }
