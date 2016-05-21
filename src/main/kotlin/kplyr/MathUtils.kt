// scalar operations
// remove because we must work with lists here
//infix operator fun DoubleArray.plus(i: Int): DoubleArray = map { it + i }.toDoubleArray()
// todo this could also be an extension property
fun List<Number>.mean(): Double = map { it.toDouble() }.sum() / size


// http://stackoverflow.com/questions/4662292/scala-median-implementation
fun List<Double>.median(removeNA: Boolean = false): Double {
    val (lower, upper) = sorted().let { take(size / 2) to takeLast(size / 2) }
    return if (size % 2 == 0) (lower.last() + upper.first()) / 2.0 else upper.first()
}


fun List<Number>.sd(): Double = Math.sqrt((map { it.toDouble() * it.toDouble() } - mean()).sum() / size)

// inspired by http://stackoverflow.com/questions/3224935/in-scala-how-do-i-fold-a-list-and-return-the-intermediate-results
fun <T : Number> List<T>.cumSum(removeNA: Boolean = false): Iterable<Double> {
    return drop(1).fold(listOf(first().toDouble()), { list, curVal -> list + (list.last().toDouble() + curVal.toDouble()) })
}

//
//def quantile(quantile:Double) = {
//    assert(quantile >=0 && quantile <=1)
//    // convert quantile into and index
//    val quantIndex: Int = (values.length.toDouble*quantile).round.toInt -1
//    values.sorted.toList(quantIndex)
//}