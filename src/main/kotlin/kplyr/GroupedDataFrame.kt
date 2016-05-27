package kplyr

import cumSum


// To illustrate the structure of th API just core verbs are implemented as instance functions. The rest is implement as extension functions.

internal data class GroupIndex(val groupHash: Int, @Deprecated("unused") val rowIndices: IntArray)

internal data class DataGroup(val groupHash: Int, val df: DataFrame)


internal class GroupedDataFrame(val by: List<String>, internal val groups: List<DataGroup>) : DataFrame {

    override val rows: Iterable<Map<String, Any?>> = object : Iterable<Map<String, Any?>> {
        override fun iterator() = object : Iterator<Map<String, Any?>> {
            var curRow = 0

            override fun hasNext(): Boolean = curRow < nrow

            override fun next(): Map<String, Any?> = row(curRow++)
        }
    }

    override val names: List<String>
        get() = groups.first().df.names


    override val ncol: Int
        get() = groups.first().df.ncol


    internal val groupOffsets: List<Int> by lazy {
        val groupSizes = groups.map { it.df.nrow }
        (listOf(0) + groupSizes.dropLast(1)).cumSum().toList().map { it.toInt() }
    }

    override fun row(rowIndex: Int): Map<String, Any?> {
        val grpIndex = groupOffsets.filter { it <= rowIndex }.size - 1
        val rowOffset = groupOffsets.filter { it <= rowIndex }.last()

        return groups[grpIndex].df.row(rowIndex - rowOffset)
    }


    override fun get(name: String): DataCol = ungroup()[name]


    override val nrow: Int
        get() = groups.map { it.df.nrow }.sum()


    override fun summarize(vararg sumRules: TableFormula): DataFrame {
        return groups.map {
            it.df.summarize(*sumRules)
        }.bindRows() // todo does dplyr keep the group here?? .groupBy(*by.toTypedArray())
    }

    // fixme get rid of rbind.groupby anti-pattern in most core-verbs

    override fun select(which: List<Boolean>): DataFrame {
        return groups.map { it.df.select(which) }.bindRows().groupBy(*by.toTypedArray())
    }


    override fun filter(predicate: DataFrame.(DataFrame) -> BooleanArray): DataFrame {
        return groups.map { it.df.filter(predicate) }.bindRows().groupBy(*by.toTypedArray())
    }

    override fun mutate(tf: TableFormula): DataFrame {
        return groups.map { it.df.mutate(tf) }.bindRows().groupBy(*by.toTypedArray())
    }

    override fun arrange(vararg by: String): DataFrame {
        // fixme this is not dplyr-consistent which keeps grouping index detached from global row order
        return GroupedDataFrame(this.by, groups.map { DataGroup(it.groupHash, it.df.arrange(*by)) })
    }

    override fun groupBy(vararg by: String): DataFrame = ungroup().groupBy(*by)

    override fun ungroup(): DataFrame = groups.map { it.df }.bindRows()

    override fun toString(): String = "Grouped by: *$by\n" + ungroup().head(5).asString()

    fun groups() = slice(1).ungroup().select(*by.toTypedArray())
}
