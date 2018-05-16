package krangl

import java.text.DecimalFormat


// scalar operations
// remove because we must work with lists here
//infix operator fun DoubleArray.plus(i: Int): DoubleArray = map { it + i }.toDoubleArray()
// todo this could also be an extension property
fun Array<Double>.mean(): Double = map { it.toDouble() }.sum() / size

fun List<Double>.mean(): Double = map { it.toDouble() }.sum() / size


// from http://stackoverflow.com/questions/23086291/format-in-kotlin-string-templates
//internal fun Double.format(digits: Int) = java.lang.String.format("%.${digits}f", this)
//internal fun Double.format(digits: Int) = java.lang.String.format("%.${"#".repeat(digits)}", this)
// for scientific notation see https://stackoverflow.com/questions/2944822/format-double-value-in-scientific-notation
internal fun Double.format(digits: Int) = DecimalFormat("#.${"#".repeat(digits)}").format(this)

// http://stackoverflow.com/questions/4662292/scala-median-implementation
fun Array<Double>.median(): Double {
    val (lower, upper) = sorted().let { take(size / 2) to takeLast(size / 2) }
    return if (size % 2 == 0) (lower.last() + upper.first()) / 2.0 else upper.first()
}


fun Array<Double>.sd() = if (size == 1) null else Math.sqrt(map { Math.pow(it.toDouble() - mean(), 2.toDouble()) }.sum() / size.toDouble())

// inspired by http://stackoverflow.com/questions/3224935/in-scala-how-do-i-fold-a-list-and-return-the-intermediate-results
fun <T : Number> List<T>.cumSum(): Iterable<Double> {
    return drop(1).fold(listOf(first().toDouble()), { list, curVal -> list + (list.last().toDouble() + curVal.toDouble()) })
}

//
//def quantile(quantile:Double) = {
//    assert(quantile >=0 && quantile <=1)
//    // convert quantile into and index
//    val quantIndex: Int = (values.length.toDouble*quantile).round.toInt -1
//    values.sorted.toList(quantIndex)
//}