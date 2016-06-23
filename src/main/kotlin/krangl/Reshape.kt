package krangl

/**
 * Spread a key-value pair across multiple columns.
 *
 * @param key The bare (unquoted) name of the column whose values will be used as column headings.
 * @param value The bare (unquoted) name of the column whose values will populate the cells.
 * @param fill If set, missing values will be replaced with this value.
 * @param convert If set, automatic type conversion will be run on all new columns. This is useful if the value column
 *                was a mix of variables that was coerced to a string.
 */
fun DataFrame.spread(key: String, value: String, fill: Any? = null, convert: Boolean = false): DataFrame {

    // create new columns
    val newColNames = this[key].values().distinct()  // .map { it.toString() } dont'convert already because otherwise join will fail

    // make sure that new column names do not exist already
    require(names.intersect(newColNames).isEmpty()) { "spread columns do already exist in data-frame" }


    val bySpreadGroup = groupBy(*names.minus(listOf(key, value)).toTypedArray()) as GroupedDataFrame

    // todo use big initially empty array here and fill it with spread data
    val spreadGroups: List<DataFrame> = bySpreadGroup
            .groups
            .map {
                val grpDf = it.df

                require(grpDf.select(key).distinct(key).nrow == grpDf.nrow) { "key value mapping is not unique" }

                val spreadBlock = SimpleDataFrame(handleListErasure(key, newColNames)).leftJoin(grpDf.select(key, value))

                val grpSpread = SimpleDataFrame((spreadBlock as SimpleDataFrame).rows.map {
                    AnyCol(it[key].toString(), listOf(it[value]))
                })

                bindCols(grpDf.select(-key, -value).distinct(), grpSpread)
            }

//    if(fill!=null){
//        spreadBlock =  spreadBlock.
//    }

    val spreadWithGHashes = spreadGroups.bindRows()


    // coerce types of strinified coluymns similar to how tidy is doing things
    var typeCoercedSpread = newColNames.map { it.toString() }
            .foldRight(spreadWithGHashes, { spreadCol, df ->
                df.mutate(spreadCol to { handleArrayErasure(spreadCol, df[spreadCol].values()) })
            })

    if (convert) {
        typeCoercedSpread = newColNames
                // stringify spread column names
                .map { it.toString() }

                // select for String-type coluymns
                .filter { typeCoercedSpread[it] is StringCol }

                // attempt conversion
                .foldRight(typeCoercedSpread, { spreadCol, df ->
                    convertType(df, spreadCol)
                })

    }

    return typeCoercedSpread
}


/** Gather takes multiple columns and collapses into key-value pairs, duplicating all other columns as needed. You use
 * gather() when you notice that you have columns that are not variables.
 *
 * @param key Name of the key column to create in output.
 * @param value Name of the value column to create in output.
 * @param which The colums to gather. The same selectar syntax as for `krangl::select` is supported here
 * @param convert If TRUE will automatically run `convertType` on the key column. This is useful if the
 *                column names are actually numeric, integer, or logical.
 */

fun DataFrame.gather(key: String, value: String, convert: Boolean = false, vararg which: String = names.toTypedArray()): DataFrame =
        gather(key, value, convert, if (which.isEmpty()) this else this.select(*which))

fun DataFrame.gather(key: String, value: String, convert: Boolean = false, vararg which: ColNames.() -> List<Boolean?>): DataFrame =
        gather(key, value, convert, if (which.isEmpty()) this else this.select(*which))


// internal api only

internal fun DataFrame.gather(key: String, value: String, convert: Boolean = false, gatherColumns: DataFrame): DataFrame {

    // 1) convert each gather column into a block
    val distinctCols = gatherColumns.cols.map { it.javaClass }.distinct()


    // todo why cant we use handleArrayErasure() here?
    fun makeValueCol(name: String, data: Array<*>): DataCol = when {
        distinctCols == IntCol::class.java -> IntCol(name, data as List<Int?>)
        distinctCols == DoubleCol::class.java -> DoubleCol(name, data as List<Double?>)
        distinctCols == BooleanCol::class.java -> BooleanCol(name, data as List<Boolean?>)
        else -> StringCol(name, Array(data.size, { index -> data[index]?.toString() }))
    }

    val gatherBlock = gatherColumns.cols.map { column ->
        SimpleDataFrame(
                StringCol(key, Array(column.length, { column.name as String? })),
                makeValueCol(value, column.values())
        )
    }.bindRows().let {
        // optionally try to convert key column
        if (convert) convertType(it, key) else it
    }


    // 2) row-replicate the non-gathered columns
    //    val rest = select(names.minus(gatherColumns.names))
    val rest = select({ -oneOf(gatherColumns.names) })
//    val replicationLevel = gatherColumns.ncol

    val indexReplication = rest.cols.map { column ->
        handleArrayErasure(column.name, Array(gatherBlock.nrow, { index -> column.values()[index.mod(column.length)] }))
    }.run { SimpleDataFrame(this) }


    // 3) combined the gather-block with the replicated index-data
    return bindCols(indexReplication, gatherBlock)
}


/** Convert a character vector to logical, integer, numeric, complex or factor as appropriate. See tidyr::type.convert */
internal fun convertType(df: DataFrame, spreadCol: String): DataFrame {
    val columnData = df[spreadCol].asStrings()
    val firstElements = columnData.take(20).toList()

    val convColumn: DataCol = when {
        isIntCol(firstElements) -> IntCol(spreadCol, columnData.map { it?.toInt() })
        isDoubleCol(firstElements) -> DoubleCol(spreadCol, columnData.map { it?.toDouble() })
        isBoolCol(firstElements) -> BooleanCol(spreadCol, columnData.map { it?.toBoolean() })
        else -> df[spreadCol]
    }

    return df.mutate(spreadCol to { convColumn })
}
