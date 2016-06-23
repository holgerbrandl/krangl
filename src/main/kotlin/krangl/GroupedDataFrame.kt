package krangl


// To illustrate the structure of th API just core verbs are implemented as instance functions. The rest is implement as extension functions.

internal data class GroupIndex(val groupHash: Int, val rowIndices: IntArray)

internal class DataGroup(val groupHash: Int, val df: DataFrame) {
    override fun toString(): String {
        return "DataGroup($groupHash)" // just needed for debugging
    }
}


internal class GroupedDataFrame(val by: List<String>, internal val groups: List<DataGroup>) : DataFrame {

    override val rawRows: Iterable<List<Any?>>
        get() = throw UnsupportedOperationException()

    // todo simple aggregate group-row-iterators into compound iterator to prevent indexed access
    override val rows: Iterable<Map<String, Any?>> = object : Iterable<Map<String, Any?>> {
        override fun iterator() = object : Iterator<Map<String, Any?>> {
            var curRow = 0

            override fun hasNext(): Boolean = curRow < nrow

            override fun next(): Map<String, Any?> = row(curRow++)
        }
    }


    override val nrow: Int
        get() = groups.map { it.df.nrow }.sum()


    override val ncol: Int
        get() = groups.first().df.ncol

    override val names: List<String>
        get() = groups.first().df.names


    override val cols: List<DataCol>
        get() = ungroup().cols


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


    override fun summarize(vararg sumRules: TableFormula): DataFrame {
        // supposedly slow old implementation
//        return groups.map {
//            val groupSumRules: List<TableFormula> = by.map {
//                groupAttr -> TableFormula(groupAttr, { it[groupAttr].values().first() })
//            }
//            it.df.summarize(*groupSumRules.toTypedArray(), *sumRules)
//        }.bindRows() // todo does dplyr keep the group here?? .groupBy(*by.toTypedArray())

        // todo conisder to expose the group tuple via public API
        return groups.map { gdf ->
            val groupTuple = gdf.df.select(*by.toTypedArray()).head(1)
            val groupSummary = gdf.df.summarize(*sumRules)

            bindCols(groupTuple, groupSummary)
        }.bindRows()
    }


    override fun select(which: List<String>): DataFrame {
        // see https://github.com/hadley/dplyr/issues/1869
//        require(which.intersect(by).isEmpty()) { "can not drop grouping columns" }
        warning(by.minus(which).isEmpty()) {
            "Adding missing grouping variables: ${by.minus(which).joinToString(",")}"
        }


        val groupsAndWhich = by.toMutableList().apply { addAll(which.minus(by)) }
        return GroupedDataFrame(by, groups.map { DataGroup(it.groupHash, it.df.select(groupsAndWhich)) })
    }


    // fixme get rid of rbind.groupby anti-pattern in most core-verbs

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

    override fun groupBy(vararg by: String): DataFrame =
            ungroup().groupBy(*by)
//            if(by.toList().sorted() ==this.by.sorted()) this else ungroup().groupBy(*by)

    override fun ungroup(): DataFrame = groups.map { it.df }.bindRows()

    override fun toString(): String = "Grouped by: *$by\n" + ungroup().head(5).asString()

    fun groups() = slice(1).ungroup().select(*by.toTypedArray())
}
