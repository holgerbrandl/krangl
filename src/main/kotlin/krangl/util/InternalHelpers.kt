package krangl.util

import krangl.*

/**
 * @author Holger Brandl
 */

fun List<DataCol>.asDF(): DataFrame = SimpleDataFrame(this)


fun DataCol.createComparator(): java.util.Comparator<Int> {
    return when (this) {
    // todo use nullsLast
        is DoubleCol -> Comparator { left, right -> nullsLast<Double>().compare(values[left], values[right]) }
        is IntCol -> Comparator { left, right -> nullsLast<Int>().compare(values[left], values[right]) }
        is BooleanCol -> Comparator { left, right -> nullsLast<Boolean>().compare(values[left], values[right]) }
        is StringCol -> Comparator { left, right -> nullsLast<String>().compare(values[left], values[right]) }
        is AnyCol -> Comparator { left, right ->
            @Suppress("UNCHECKED_CAST")
            nullsLast<Comparable<Any>>().compare(values[left] as Comparable<Any>, values[right] as Comparable<Any>)
        }
        else -> throw UnsupportedOperationException()
    }
}
