package kplyr

import kplyr.JoinType.*

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
    LEFT, RIGHT, INNER, OUTER
}
//

// todo more consistent wrappers needed

/** Convenience wrapper around <code>joinInner</code> that works with single single by attribute.*/


//
// Left Join
//

/** Convenience wrapper around <code>joinLeft</code> that works with single single by attribute.*/

fun leftJoin(left: DataFrame, right: DataFrame, by: String, suffices: Pair<String, String> = ".x" to ".y") =
        join(left, right, listOf(by), suffices, LEFT)

fun leftJoin(left: DataFrame, right: DataFrame, by: Iterable<String> = defaultBy(left, right), suffices: Pair<String, String> = ".x" to ".y") =
        join(left, right, by, suffices, LEFT)


//
// Inner Join
//


fun innerJoin(left: DataFrame, right: DataFrame, by: String, suffices: Pair<String, String> = ".x" to ".y") =
        join(left, right, listOf(by), suffices, INNER)


fun innerJoin(left: DataFrame, right: DataFrame, by: Iterable<String> = defaultBy(left, right), suffices: Pair<String, String> = ".x" to ".y") =
        join(left, right, by, suffices, INNER)

//
// Semi Join: Special case of inner join against distinct right side
//

fun semiJoin(left: DataFrame, right: DataFrame, by: String) = semiJoin(left, right, listOf(by))

fun semiJoin(left: DataFrame, right: DataFrame, by: Iterable<Pair<String, String>>) =
        semiJoin(left, resolveUnequalBy(right, by), by.toMap().keys)

fun semiJoin(left: DataFrame, right: DataFrame, by: Iterable<String> = defaultBy(left, right), suffices: Pair<String, String> = ".x" to ".y"): DataFrame {
    val rightReduced = right
            // just keep one instance per group
            .distinct(*by.toList().toTypedArray()) //  slow for bigger data (because grouped here and later again)??
            // remove non-grouping columns to prevent columns suffixing
            .select(*by.toList().toTypedArray())

    return join(left, rightReduced, by, suffices, INNER)
}


//
// Outer Join: Special case of inner join against distinct right side
//


fun joinOuter(left: DataFrame, right: DataFrame, by: Iterable<String> = defaultBy(left, right)) =
        join(left, right, by, type = OUTER)


object UnequalByHelpers {

    fun innerJoin(left: DataFrame, right: DataFrame, by: Iterable<Pair<String, String>>, suffices: Pair<String, String> = ".x" to ".y") =
            join(left, resolveUnequalBy(right, by), by.toMap().keys, suffices, INNER)


}


fun join(left: DataFrame, right: DataFrame, by: Iterable<String> = defaultBy(left, right), suffices: Pair<String, String> = ".x" to ".y", type: JoinType): DataFrame {

    val (groupedLeft, groupedRight) = prep4Join(by, left, right, suffices)

    val rightIt = groupedRight.groups.iterator()
    val leftIt = groupedLeft.groups.iterator()

    val groupZipper = mutableListOf<Pair<DataGroup?, DataGroup?>>()

    fun <T> Iterator<T>.nextOrNull(): T? = if (hasNext()) next() else null

    var rightGroup = rightIt.nextOrNull() // stdlib method??

    leftLoop@
    while (leftIt.hasNext()) {
        val leftGroup = leftIt.next()

        rightLoop@
        while (rightGroup != null) {

            if (leftGroup.groupHash < rightGroup.groupHash) {
                // right is ahead of left
                groupZipper.add(leftGroup to null)
                continue@leftLoop

            } else if (leftGroup.groupHash == rightGroup.groupHash) {
                groupZipper.add(leftGroup to rightGroup)
                rightGroup = rightIt.nextOrNull()
                continue@leftLoop

            } else {
                // left is ahead of right
                groupZipper.add(null to rightGroup)
                rightGroup = rightIt.nextOrNull()
                continue@rightLoop
            }
        }

        // consume unpaired right blocks
        groupZipper.add(leftGroup to null)
    }

    // consume rest of right table iterator
    while (rightGroup != null) {
        groupZipper.add(null to rightGroup)
        rightGroup = rightIt.nextOrNull()
    }


    // depending on join type build cartesian products and finish

    val filterZipper = when (type) {
        LEFT -> groupZipper.filter { it.first != null }
        RIGHT -> groupZipper.filter { it.second != null }
        INNER -> groupZipper.filter { it.first != null && it.second != null }
        OUTER -> groupZipper
    }


    // prepare "overhang null-filler blocks" for cartesian products
    // note: `left` as argument is not enough here because of column shuffling and suffixing
    val leftNull = nullRow(groupedLeft.groups.first().df)
    val rightNull = nullRow(groupedRight.groups.first().df)


    // todo this could be multi-threaded but be careful to ensure deterministic order
    val mergedGroups = mutableListOf<DataFrame>()

    for ((unzipLeft, unzipRight) in filterZipper) {
        cartesianProduct(unzipLeft?.df ?: leftNull, unzipRight?.df ?: rightNull, by.toList()).let { mergedGroups.add(it) }
    }

    // todo use more efficient row-bind implementation here
    val header = bindCols(leftNull, rightNull.select(-by)).head(0)
    return mergedGroups.fold(header, { left, right -> listOf(left, right).bindRows() })
//    return mergedGroups.reduce { left, right -> listOf(left, right).bindRows() }
}


//
// Internal utility methods for join implementation
//


/** rename second to become compliant with first. */
private fun resolveUnequalBy(df: DataFrame, by: Iterable<Pair<String, String>>): DataFrame {
    // just do something if the pairs are actually unqueal
//    if (by.count { it.first != it.second } == 0) return df

    return by.map { it.second to it.first }.fold(df, { df, curBy -> df.rename(curBy) })
}


/** Given a data-frame, this method derives a 1-row table with the same colum types but null as value for all columns. */
private fun nullRow(df: DataFrame): DataFrame = (df as SimpleDataFrame).cols.fold(SimpleDataFrame(), { nullDf, column ->

    when (column) {
        is IntCol -> IntCol(column.name, listOf(null))
        is StringCol -> StringCol(column.name, listOf(null))
        is BooleanCol -> BooleanCol(column.name, listOf(null))
        is DoubleCol -> DoubleCol(column.name, listOf(null))
        else -> AnyCol(column.name, listOf(null))

    }.let { nullDf.addColumn(it) }
})


private fun addSuffix(df: DataFrame, cols: Iterable<String>, prefix: String = "", suffix: String = ""): DataFrame {
    val renameRules = cols.map { RenameRule(it, prefix + it + suffix) }
    return df.rename(*renameRules.toTypedArray())
}


private fun prep4Join(by: Iterable<String>, left: DataFrame, right: DataFrame, suffices: Pair<String, String>): Pair<GroupedDataFrame, GroupedDataFrame> {

    // detect common no-by columns and apply optional suffixing
    val toBeSuffixed = left.names.intersect(right.names).minus(by)

    val groupedLeft = (addSuffix(left, toBeSuffixed, suffix = suffices.first)
            // move join columns to the left
            .run { select(by.toMutableList().apply { addAll(names.minus(by)) }) }
            .groupBy(*by.toList().toTypedArray()) as GroupedDataFrame).hashSorted()


    val groupedRight = (addSuffix(right, toBeSuffixed, suffix = suffices.second)
            // move join columns to the left
            .run { select(by.toMutableList().apply { addAll(names.minus(by)) }) }
            .groupBy(*by.toList().toTypedArray()) as GroupedDataFrame).hashSorted()

    return Pair(groupedLeft, groupedRight)
}


//fun DataFrame.joinLeft(right: DataFrame,by:String) = joinLeft(this, right, *by)
private fun defaultBy(left: DataFrame, right: DataFrame) = left.names.intersect(right.names).apply {
    System.err.print("""Joining by: ${this.joinToString(",")}""")
}


private fun cartesianProduct(left: DataFrame, right: DataFrame, removeFromRight: List<String>): DataFrame {
    // first remove columns that are present in both from right-df
    val rightSlim = right.select({ oneOf(*removeFromRight.toTypedArray()).nullAsFalse().map { !it } })

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