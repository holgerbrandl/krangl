// scalar operations
// remove because we must work with lists here
//infix operator fun DoubleArray.plus(i: Int): DoubleArray = map { it + i }.toDoubleArray()
// todo this could also be an extension property
fun List<Number>.mean(): Double = map { it.toDouble() }.sum() / size

fun List<Number>.sd(): Double = Math.sqrt((map { it.toDouble() * it.toDouble() } - mean()).sum() / size)


// todo convert from scala
// http://stackoverflow.com/questions/4662292/scala-median-implementation
//def median = {
//    val (lower, upper) = values.sorted.splitAt(values.size / 2)
//    if (values.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head
//}
//
//def quantile(quantile:Double) = {
//    assert(quantile >=0 && quantile <=1)
//    // convert quantile into and index
//    val quantIndex: Int = (values.length.toDouble*quantile).round.toInt -1
//    values.sorted.toList(quantIndex)
//}