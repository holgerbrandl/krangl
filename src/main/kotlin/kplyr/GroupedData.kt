package kplyr


// To illustrate the structure of th API just core verbs are implemented as instance functions. The rest is implement as extension functions.

internal data class GroupIndex(val groupHash: Int, @Deprecated("unused") val rowIndices: IntArray)

internal data class DataGroup(val groupHash: Int, val df: DataFrame)


internal class GroupedDataFrame(private val by: List<String>, private val groups: List<DataGroup>) : DataFrame {

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


    val cumIndex: List<Int> by lazy { groups.map { it.df.nrow }.toList().cumSum().toList() }

    override fun row(rowIndex: Int): Map<String, Any?> {
        val grpIndex = cumIndex.filter { it <= rowIndex }.size
        val rowOffset = cumIndex.filter { it <= rowIndex }.last()

        return groups[grpIndex].df.row(rowIndex - rowOffset)
    }


    override fun get(name: String): DataCol = ungroup()[name]


    override val nrow: Int
        get() = groups.map { it.df.nrow }.sum()


    override fun summarize(vararg sumRules: Pair<String, DataFrame.(DataFrame) -> Any?>): DataFrame {
        return groups.map {
            val groupSumRules: List<Pair<String, DataFrame.(DataFrame) -> Any?>> = by.map {
                groupAttr ->
                Pair<String, DataFrame.(DataFrame) -> Any?>(groupAttr, { it[groupAttr].values().first() })
            }
            it.df.summarize(*groupSumRules.toTypedArray(), *sumRules)
        }.rbind().groupBy(*by.toTypedArray())
    }

    // fixme get rid of rbind.groupby anti-pattern in most core-verbs

    override fun select(which: List<Boolean>): DataFrame {
        return groups.map { it.df.select(which) }.rbind().groupBy(*by.toTypedArray())
    }


    override fun filter(predicate: DataFrame.(DataFrame) -> BooleanArray): DataFrame {
        return groups.map { it.df.filter(predicate) }.rbind().groupBy(*by.toTypedArray())
    }

    override fun mutate(name: String, formula: DataFrame.(DataFrame) -> Any?): DataFrame {
        return groups.map { it.df.mutate(name, formula) }.rbind().groupBy(*by.toTypedArray())
    }

    override fun arrange(vararg by: String): DataFrame {
        // fixme this is not dplyr-consistent which keeps grouping index detached from global row order
        return GroupedDataFrame(this.by, groups.map { DataGroup(it.groupHash, it.df.arrange(*by)) })
    }

    override fun groupBy(vararg by: String): DataFrame = ungroup().groupBy(*by)

    override fun ungroup(): DataFrame = groups.map { it.df }.rbind()

    override fun toString(): String = "Grouped by: *$by\n" + ungroup().head(5).asString()

}


// inspired by http://stackoverflow.com/questions/3224935/in-scala-how-do-i-fold-a-list-and-return-the-intermediate-results
internal fun <T> List<T>.cumSum(): Iterable<T> {
    return drop(1).fold(listOf(first()), { list, curVal -> list + list.last() + curVal })
}

/** Concatenate a list of data-frame by row. */
fun List<DataFrame>.rbind(): DataFrame { // add options about NA-fill over non-overlapping columns
    // todo more column model consistency checks here
    // note: use fold to bind with non-overlapping column model

    val bindCols = mutableListOf<DataCol>()

    val totalRows = map { it.nrow }.sum()

    for (colName in this.first().names) {
        val colDataCombined = Array(totalRows, { bindColData(colName)[it] })

        when (this.first()[colName]) {
            is DoubleCol -> DoubleCol(colName, colDataCombined.map { it as Double })
            is IntCol -> IntCol(colName, colDataCombined.map { it as Int })
            is StringCol -> StringCol(colName, colDataCombined.map { it as String })
            is BooleanCol -> BooleanCol(colName, colDataCombined.map { it as Boolean })
            else -> throw UnsupportedOperationException()
        }.apply { bindCols.add(this) }

    }

    return SimpleDataFrame(bindCols)
}

private fun List<DataFrame>.bindColData(colName: String): List<*> {
    val map = map {
        it[colName].values()
    }
    return map.reduce { accu, curEl -> accu.toMutableList().apply { addAll(curEl) }.toList() }
}