package krangl


// To illustrate the structure of th API just core verbs are implemented as instance functions. The rest is implement as extension functions.

internal data class GroupIndex(val groupHash: GroupKey, val rowIndices: IntArray)

internal class DataGroup(val groupKey: GroupKey, val df: DataFrame) {
    override fun toString(): String {
        return "DataGroup($groupKey)" // just needed for debugging
    }
}


internal class GroupedDataFrame(val by: List<String>, internal val groups: List<DataGroup>) : DataFrame {


    // todo simple aggregate group-row-iterators into compound iterator to prevent indexed access
    override val rows: Iterable<DataFrameRow> = object : Iterable<DataFrameRow> {
        override fun iterator() = object : Iterator<DataFrameRow> {
            var curRow = 0

            override fun hasNext(): Boolean = curRow < nrow

            override fun next(): DataFrameRow = row(curRow++)
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

    override fun row(rowIndex: Int): DataFrameRow {
        val grpIndex = groupOffsets.filter { it <= rowIndex }.size - 1
        val rowOffset = groupOffsets.filter { it <= rowIndex }.last()

        return groups[grpIndex].df.row(rowIndex - rowOffset)
    }


    override fun get(columnName: String): DataCol = ungroup()[columnName]


    override fun summarize(vararg sumRules: ColumnFormula): DataFrame {
        // supposedly slow old implementation
//        return groups.map {
//            val groupSumRules: List<ColumnFormula> = by.map {
//                groupAttr -> ColumnFormula(groupAttr, { it[groupAttr].values().first() })
//            }
//            it.df.summarize(*groupSumRules.toTypedArray(), *sumRules)
//        }.bindRows() // todo does dplyr keep the group here?? .groupBy(*by.toTypedArray())

        // todo conisder to expose the group tuple via public API
        return groups.map { gdf ->
            val groupTuple = gdf.df.select(*by.toTypedArray()).take(1)
            val groupSummary = gdf.df.summarize(*sumRules)

            bindCols(groupTuple, groupSummary, renameDuplicates = false)
        }.bindRows()
    }


    override fun select(vararg columns: String): DataFrame {
        // see https://github.com/hadley/dplyr/issues/1869
//        require(columns.intersect(by).isEmpty()) { "can not drop grouping columns" }
        warnIf(by.minus(columns).isEmpty()) {
            "Adding missing grouping variables: ${by.minus(columns).joinToString(",")}"
        }

        val groupsAndWhich = by.toMutableList().apply { addAll(columns.asList().minus(by)) }
        return GroupedDataFrame(by, groups.map { DataGroup(it.groupKey, it.df.select(groupsAndWhich)) })
    }

    // fixme get rid of rbind.groupby anti-pattern in most core-verbs

    override fun filter(predicate: VectorizedRowPredicate): DataFrame {
        return groups.map { it.df.filter(predicate) }.bindRows().groupBy(*by.toTypedArray())
    }

    override fun addColumn(tf: ColumnFormula): DataFrame {
        return groups.map { it.df.addColumn(tf) }.bindRows().groupBy(*by.toTypedArray())
    }

    override fun sortedBy(vararg by: String): DataFrame {
        // fixme this is not dplyr-consistent which keeps grouping index detached from global row order
        return GroupedDataFrame(this.by, groups.map { DataGroup(it.groupKey, it.df.sortedBy(*by)) })
    }

    override fun groupBy(vararg by: String): DataFrame =
            ungroup().groupBy(*by)
//            if(by.toList().sorted() ==this.by.sorted()) this else ungroup().groupBy(*by)

    override fun ungroup(): DataFrame = groups.map { it.df }.bindRows()

    override fun toString(): String = "Grouped by: *$by$lineSeparator" + ungroup().take(5).asString()

    override fun groupedBy() = slice(1).ungroup().select(*by.toTypedArray())

    // todo add nest() support
    override fun groups(): List<DataFrame> = groups.map { it.df }
}
