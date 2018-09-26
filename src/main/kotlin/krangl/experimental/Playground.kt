package krangl.experimental

import krangl.DataFrame
import krangl.unfold
import krangl.unnest

/**
 * Unfold all list columns vertically and all objects properties horizontally.
 * @author Holger Brandl
 */
fun DataFrame.flatten(): DataFrame {


    // first, unnest all list columns vertically
    val vertFlat = names.fold(this) { df, colName ->
        val firstVal = df[colName].values().asSequence().filterNotNull().firstOrNull()

        if (firstVal is List<*> || firstVal is DataFrame) df.unnest(colName) else df
    }


    // second, unfold object properties horizontally
    val vertHoriFlat = vertFlat.names.fold(vertFlat) { df, colName ->
        val firstVal = df[colName].values().asSequence().filterNotNull().firstOrNull()

        if (firstVal == null) return df

        // todo could refac unfold to work with Kclass parameter, because it's the only thing we have in here
        // and the type parameter is not even needed, because we can get the type from the first object in the column

        // may another flavor of unfold may also accept actual method references:     val kFunction1 = UUID::node
        //            val createInstance = firstVal::class.createInstance()
        //            val type = firstVal::class.javaObjectType

//        val type = firstVal::class.starProjectedType

        df.unfold<Any>(columnName = "dfo")
    }

    return vertHoriFlat
}


internal fun <T, K, L> cartesianProduct(left: Iterable<T>, right: Iterable<K>, transform: (T, K) -> L): Iterable<L> {
    return left.flatMap { someT -> right.map { someK -> transform(someT, someK) } }
}

// see https://youtrack.jetbrains.com/issue/KT-27028
internal object DoubleSum {

    @JvmStatic
    fun main(args: Array<String>) {
        val cartesianProduct = cartesianProduct(1..4, 4..20) { i, j -> 2 * i * j }.sum()
    }
}
