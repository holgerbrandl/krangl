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

//fun innerJoin(left: DataFrame, right: DataFrame,by: Iterable<String> = defaultBy(left, right)) = join(left, right, JoinType.INNER, *by)
//
//fun leftJoin(left: DataFrame, right: DataFrame,by: Iterable<String> = defaultBy(left, right)) = join(left, right, JoinType.LEFT, *by)
//
//fun rightJoin(left: DataFrame, right: DataFrame,by: Iterable<String> = defaultBy(left, right)) = join(right, left, JoinType.LEFT, *by, suffices = ".y" to ".x")
//
//fun antiJoin(left: DataFrame, right: DataFrame,by: Iterable<String> = defaultBy(left, right)) = join(left, right, JoinType.ANTI, *by)


//fun outerJoin(left: DataFrame, right: DataFrame,by: Iterable<String> = defaultBy(left, right)) = join(left, right, JoinType.LEFT, *by)


internal fun addSuffix(df: DataFrame, cols: Iterable<String>, prefix: String = "", suffix: String = ""): DataFrame {
    val renameRules = cols.map { RenameRule(it, prefix + it + suffix) }
    return df.rename(*renameRules.toTypedArray())
}


internal fun joinOuter(left: DataFrame, right: DataFrame, by: Iterable<String> = defaultBy(left, right), suffices: Pair<String, String> = ".x" to ".y"): DataFrame {
    return left // todo implement me
}


// convencience function with single by withou vararg
internal fun joinLeft(left: DataFrame, right: DataFrame, by: String, suffices: Pair<String, String> = ".x" to ".y") =
        joinLeft(left, right, listOf(by), suffices)

internal fun joinInner(left: DataFrame, right: DataFrame, by: String, suffices: Pair<String, String> = ".x" to ".y") =
        joinInner(left, right, listOf(by), suffices)


internal fun joinInner(left: DataFrame, right: DataFrame, by: Iterable<String> = defaultBy(left, right), suffices: Pair<String, String> = ".x" to ".y"): DataFrame {

    // detect common no-by columns and apply optional suffixing
    val toBeSuffixed = left.names.intersect(right.names).minus(by)

    val groupedLeft = (addSuffix(left, toBeSuffixed, suffices.first).groupBy(*by.toList().toTypedArray()) as GroupedDataFrame).hashSorted()
    val groupedRight = (addSuffix(right, toBeSuffixed, suffices.first).groupBy(*by.toList().toTypedArray()) as GroupedDataFrame).hashSorted()

    // start sorted hash-join
    val rightIt = groupedRight.groups.iterator()

    var matchRGroup: DataGroup? = rightIt.next()

    val mergedGroups = mutableListOf<DataFrame>()

    for (leftGroup in groupedLeft.groups) {
        // if group is present in A build cross-product other wise fill with NA
        if (leftGroup.groupHash == matchRGroup?.groupHash) {
            val crossProd = cartesianProduct(leftGroup.df, matchRGroup!!.df, by.toList())
            mergedGroups.add(crossProd)
        } else {
            continue
        }

        matchRGroup = if (rightIt.hasNext()) rightIt.next() else null // can happen if a group around the end of L is not present in B
    }

    return mergedGroups.reduce { left, right -> listOf(left, right).bindRows() }
}

internal fun joinLeft(left: DataFrame, right: DataFrame, by: Iterable<String> = defaultBy(left, right), suffices: Pair<String, String> = ".x" to ".y"): DataFrame {

    // detect common no-by columns and apply optional suffixing
    val toBeSuffixed = left.names.intersect(right.names).minus(by)

    val groupedLeft = (addSuffix(left, toBeSuffixed, suffix = suffices.first).groupBy(*by.toList().toTypedArray()) as GroupedDataFrame).hashSorted()
    val groupedRight = (addSuffix(right, toBeSuffixed, suffix = suffices.first).groupBy(*by.toList().toTypedArray()) as GroupedDataFrame).hashSorted()

    val rightIt = groupedRight.groups.iterator()

    var matchRGroup: DataGroup? = rightIt.next()

    val mergedGroups = mutableListOf<DataFrame>()

    for (leftGroup in groupedLeft.groups) {
        // if group is present in A build cross-product other wise fill with NA
        val crossProd = if (leftGroup.groupHash == matchRGroup?.groupHash) {
            cartesianProduct(leftGroup.df, matchRGroup!!.df, by.toList())
        } else {
            // maybe fill with NA when row-binding merge groups??
            by.fold(leftGroup.df, { df, byAttr -> df.mutate(byAttr to { null }) })
        }

        mergedGroups.add(crossProd)

        matchRGroup = if (rightIt.hasNext()) rightIt.next() else null // can happen if a group around the end of L is not present in B
    }

    return mergedGroups.reduce { left, right -> listOf(left, right).bindRows() }
}


//fun DataFrame.joinLeft(right: DataFrame,by:String) = joinLeft(this, right, *by)
internal fun defaultBy(left: DataFrame, right: DataFrame) = left.names.intersect(right.names).apply {
    System.err.print("""Joining by: ${this.joinToString(",")}""")
}


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