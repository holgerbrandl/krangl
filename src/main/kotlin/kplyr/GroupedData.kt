package kplyr


// To illustrate the structure of th API just core verbs are implemented as instance functions. The rest is implement as extension functions.

internal data class GroupIndex(val groupHash: Int, val rowIndices: IntArray)

internal data class DataGroup(val index: GroupIndex, val df: DataFrame)


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


    override fun row(rowIndex: Int): Map<String, Any> {
        // find the group
//        groups.filterKeys { it.rowIndices.contains(rowIndex) }.values.
        throw UnsupportedOperationException()
    }

    override fun get(name: String): DataCol {
        // kplyr/CoreVerbsTest.kt:7
        throw UnsupportedOperationException()
    }

    override val nrow: Int
        get() = groups.map { it.df.nrow }.sum()

    override fun summarize(vararg sumRules: Pair<String, DataFrame.(DataFrame) -> Any?>): DataFrame {
        throw UnsupportedOperationException()
    }


    override fun select(which: List<Boolean>): DataFrame {
        throw UnsupportedOperationException()
    }


    override fun filter(predicate: DataFrame.(DataFrame) -> BooleanArray): DataFrame {
        throw UnsupportedOperationException()
    }

    override fun mutate(name: String, formula: DataFrame.(DataFrame) -> Any?): DataFrame {
        throw UnsupportedOperationException()
    }
//    override fun mutate(name: String, formula: DataFrame.(DataFrame) -> Any?): DataFrame {
//        throw UnsupportedOperationException()
//    }


    override fun arrange(vararg by: String): DataFrame {
        throw UnsupportedOperationException()
    }

    override fun groupBy(vararg by: String): DataFrame {

        throw UnsupportedOperationException()
    }


    override fun ungroup(): DataFrame = groups.map { it.df }.rbind()

}


/** Concatenate a list of data-frame by row. */
fun List<DataFrame>.rbind(): DataFrame { // add options about NA-fill over non-overlapping columns
    // todo more column model consistency checks here
    // note: use fold to bind with non-overlapping column model

    val bindCols = mutableListOf<DataCol>()

    val totalRows = map { it.nrow }.sum()

    for (colName in this.first().names) {
        val colDataCombined = Array(totalRows, { map { it[colName].values() }.fold(emptyList(), { a: List<*>, b -> a + b }) })

        when (this.first()[colName]) {
            is DoubleCol -> DoubleCol(colName, colDataCombined as List<Double>)
            is IntCol -> IntCol(colName, colDataCombined as List<Int>)
            is StringCol -> StringCol(colName, colDataCombined as List<String>)
            is BooleanCol -> BooleanCol(colName, colDataCombined as List<Boolean>)
            else -> throw UnsupportedOperationException()
        }.apply { bindCols + this }

    }

    return SimpleDataFrame(bindCols)
}