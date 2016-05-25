package kplyr

/**
DataFrame joining

Nice Intro
 * http://www.cburch.com/cs/340/reading/join/index.html
 * https://de.wikipedia.org/wiki/Joinalgorithmen
 * http://codeaffectionate.blogspot.de/2012/09/fun-with-cartesian-products-cartesian.html
 * https://www.informatik.hu-berlin.de/de/forschung/gebiete/wbi/teaching/archive/sose05/dbs2/slides/09_joins.pdf
 */

// needed?
enum class JoinType {
    LEFT, RIGHT, INNER, OUTER, ANTI
}

fun joinLeft(left: DataFrame, right: DataFrame, vararg by: String = defaultBy(left, right)): DataFrame {
    val groupedLeft = (left.groupBy(*by) as GroupedDataFrame).hashSorted()
    val groupedRight = (right.groupBy(*by) as GroupedDataFrame).hashSorted()

    val rightIt = groupedRight.groups.iterator()

    var matchRGroup: DataGroup? = rightIt.next()

    val mergedGroups = mutableListOf<DataFrame>()

    for (leftGroup in groupedLeft.groups) {
        // if group is present in A build cross-product other wise fill with NA
        val crossProd = if (leftGroup.groupHash == matchRGroup?.groupHash) {
            cartesianProduct(leftGroup.df, matchRGroup!!.df, by.toList())
        } else {
            // maybe fill with NA when row-binding merge groups??
            leftGroup.df
        }

        mergedGroups.add(crossProd)

        matchRGroup = if (rightIt.hasNext()) rightIt.next() else null // can happen if a group around the end of L is not present in B
    }

    return mergedGroups.reduce { left, right -> listOf(left, right).bindRows() }
}


//fun DataFrame.joinLeft(right: DataFrame, vararg by:String) = joinLeft(this, right, *by)
internal fun defaultBy(left: DataFrame, right: DataFrame): Array<String> = left.names.intersect(right.names).apply {
    System.err.print("""Joining by: ${this.joinToString(",")}""")
}.toTypedArray()


private fun cartesianProduct(left: DataFrame, right: DataFrame, removeFromRight: List<String>): DataFrame {
    // first remove columns that are present in both from right-df
    val rightSlim = right.select({ oneOf(*removeFromRight.toTypedArray()).map { !it } })

    // http://thushw.blogspot.de/2015/10/cartesian-product-in-scala.html
    //http://codeaffectionate.blogspot.de/2012/09/fun-with-cartesian-products-cartesian.html

//    val leftIndexReplication = IntArray(left.nrow*right.nrow, { index -> }
    val leftIndexReplication = (0..(right.nrow - 1)).flatMap { leftIt: Int -> IntArray(left.nrow, { it }).toList() }
    val rightIndexReplication = (0..(right.nrow - 1)).flatMap { leftIt: Int -> IntArray(left.nrow, { leftIt }).toList() }

    // replicate data
    val leftCartesian: DataFrame = replicateByIndex(left, leftIndexReplication)
    val rightCartesian: DataFrame = replicateByIndex(rightSlim, rightIndexReplication)

    return bindCols(leftCartesian, rightCartesian)
}

private fun replicateByIndex(df: DataFrame, repIndex: List<Int>): DataFrame {
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

// make sure that by-NA groups come last here (see unit tests)
private fun GroupedDataFrame.hashSorted() = groups.sortedBy { it.groupHash }.run { GroupedDataFrame(this@hashSorted.by, this) }