package kplyr

/**
DataFrame joining

Nice Intro
 * http://www.cburch.com/cs/340/reading/join/index.html
 * https://de.wikipedia.org/wiki/Joinalgorithmen
 * http://codeaffectionate.blogspot.de/2012/09/fun-with-cartesian-products-cartesian.html
 */

enum class JoinType {
    LEFT, RIGHT, INNER, OUTER, ANTI
}

//fun DataFrame.joinLeft(right: DataFrame, vararg by:String) = joinLeft(this, right, *by)
internal fun defaultBy(left: DataFrame, right: DataFrame): Array<String> = left.names.intersect(right.names).apply {
    System.err.print("""Joining by: ${this.joinToString(",")}""")
}.toTypedArray()


fun joinLeft(left: DataFrame, right: DataFrame, vararg by: String = defaultBy(left, right)): DataFrame {
    val groupedLeft = (left.groupBy(*by) as GroupedDataFrame).hashSorted()
    val groupedRight = (right.groupBy(*by) as GroupedDataFrame).hashSorted()

    val rightIt = groupedRight.groups.iterator()

    var matchRGroup: DataGroup? = rightIt.next()

    val mergedGroups = mutableListOf<DataFrame>()

    for (leftGroup in groupedLeft.groups) {
        // if group is present in A build cross-product other wise fill with NA
        val crossProd = if (leftGroup.groupHash == matchRGroup?.groupHash) {
            cartesianProduct(leftGroup.df, matchRGroup!!.df)
        } else {
            // maybe fill with NA when row-binding merge groups??
            leftGroup.df
        }

        mergedGroups.add(crossProd)

        matchRGroup = if (rightIt.hasNext()) rightIt.next() else null // can happen if a group around the end of L is not present in B
    }

    return SimpleDataFrame()

}

fun cartesianProduct(left: DataFrame, right: DataFrame): DataFrame {
    // http://thushw.blogspot.de/2015/10/cartesian-product-in-scala.html
    //http://codeaffectionate.blogspot.de/2012/09/fun-with-cartesian-products-cartesian.html

//    val leftIndexReplication = IntArray(left.nrow*right.nrow, { index -> }
    val leftIndexReplication = (1..right.nrow).flatMap { rightIt: Int -> IntArray(left.nrow, { rightIt }).toList() }
    val rightIndexReplication = (1..right.nrow).flatMap { rightIt: Int -> IntArray(left.nrow, { it }).toList() }

    // replicate data
    val leftCartesian: DataFrame = replicateByIndex(left, leftIndexReplication)
    val rightCartesian: DataFrame = replicateByIndex(right, rightIndexReplication)

    return bindCols(leftCartesian, rightCartesian)
}

fun replicateByIndex(df: DataFrame, repIndex: List<Int>): DataFrame {
    val repCols: List<DataCol> = (df as SimpleDataFrame).cols.map { it ->
        when (it) {
            is DoubleCol -> DoubleCol(it.name, Array(repIndex.size, { index -> it.values[repIndex[index]] }).toList())
            is IntCol -> IntCol(it.name, Array(repIndex.size, { index -> it.values[repIndex[index]] }).toList())
            is BooleanCol -> BooleanCol(it.name, Array(repIndex.size, { index -> it.values[repIndex[index]] }).toList())
            is StringCol -> StringCol(it.name, Array(repIndex.size, { index -> it.values[repIndex[index]] }).toList())
            else -> throw UnsupportedOperationException()
        }
    }

    return SimpleDataFrame(repCols)
}

//fun innerJoin(left: DataFrame, right: DataFrame, vararg by: String): DataFrame {
//    val groupedLeft = (left.groupBy(*by) as GroupedDataFrame).hashSorted()
//    val groupedRight = (right.groupBy(*by) as GroupedDataFrame).hashSorted()
//
//    val rightIt = groupedRight.groups.iterator()
//
//    for (leftGroup in groupedLeft.groups) {
//        if (rightIt.hasNext()) break // can happen if a group around the end of L is not present in B
//
//        val matchRGroup = rightIt.next()
//
//        // continue if group is just present in L --> inner join
//        if(matchRGroup.groupHash!=leftGroup.groupHash) continue
//
//
//    }
//
//
//    return SimpleDataFrame()
//
//}

internal fun GroupedDataFrame.hashSorted() = groups.sortedBy { it.groupHash }.run { GroupedDataFrame(this@hashSorted.by, this) }