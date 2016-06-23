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
                    val columnData = df[spreadCol].asStrings()
                    val firstElements = columnData.take(20).toList()

                    val convColumn: DataCol = when {
                        isIntCol(firstElements) -> IntCol(spreadCol, columnData.map { it?.toInt() })
                        isDoubleCol(firstElements) -> DoubleCol(spreadCol, columnData.map { it?.toDouble() })
                        isBoolCol(firstElements) -> BooleanCol(spreadCol, columnData.map { it?.toBoolean() })
                        else -> df[spreadCol]
                    }

                    df.mutate(spreadCol to { convColumn })
                })

    }

    return typeCoercedSpread
}
