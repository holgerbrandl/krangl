package krangl

/**
 * Spread a key-value pair across multiple columns.
 *
 * @param key The bare (unquoted) name of the column whose values will be used as column headings.
 * @param value The bare (unquoted) name of the column whose values will populate the cells.
 * @param fill If set, missing values will be replaced with this value.
 * @param convert If set, attempt to do a type conversion will be run on all new columns. This is useful if the value column
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

                bindCols(grpDf.remove(key, value).distinct(), grpSpread)
            }

    //    if(fill!=null){
    //        spreadBlock =  spreadBlock.
    //    }

    val spreadWithGHashes = spreadGroups.bindRows()


    // coerce types of strinified coluymns similar to how tidy is doing things
    var typeCoercedSpread = newColNames.map { it.toString() }
            .foldRight(spreadWithGHashes, { spreadCol, df ->
                df.addColumn(spreadCol to { handleArrayErasure(spreadCol, df[spreadCol].values()) })
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
 * @param columns The colums to gather. The same selectar syntax as for `krangl::select` is supported here
 * @param convert If TRUE will automatically run `convertType` on the key column. This is useful if the
 *                column names are actually numeric, integer, or logical.
 */
fun DataFrame.gather(key: String, value: String, columns: List<String> = this.names, convert: Boolean = false): DataFrame {
    require(columns.isNotEmpty()) { "the column selection to be `gather`ed must not be empty" }

    val gatherColumns = select(columns)

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
    val rest = remove(gatherColumns.names)
    //    val replicationLevel = gatherColumns.ncol

    val indexReplication = rest.cols.map { column ->
        handleArrayErasure(column.name, Array(gatherBlock.nrow, { index -> column.values()[index.mod(column.length)] }))
    }.run { SimpleDataFrame(this) }


    // 3) combined the gather-block with the replicated index-data
    return bindCols(indexReplication, gatherBlock)
}


fun DataFrame.gather(key: String, value: String,
                     columns: ColumnSelector, // no default here to avoid signature clash = { all() },
                     convert: Boolean = false
): DataFrame = gather(key, value, colSelectAsNames(reduceColSelectors(arrayOf(columns))), convert)


/**
 * Convert a character vector to logical, integer, numeric, complex or factor as appropriate.
 *
 * @see tidyr::type.convert
 */
internal fun convertType(df: DataFrame, spreadColName: String): DataFrame {
    val spreadCol = df[spreadColName]
    val convColumn: DataCol = convertType(spreadCol)

    return df.addColumn(spreadColName to { convColumn })
}

internal fun convertType(spreadCol: DataCol): DataCol {
    val columnData = spreadCol.asStrings()
    val firstElements = columnData.take(20).toList()

    val convColumn: DataCol = when {
        isIntCol(firstElements) -> IntCol(spreadCol.name, columnData.map { it?.toInt() })
        isDoubleCol(firstElements) -> DoubleCol(spreadCol.name, columnData.map { it?.toDouble() })
        isBoolCol(firstElements) -> BooleanCol(spreadCol.name, columnData.map { it?.toBoolean() })
        else -> spreadCol
    }
    return convColumn
}

/**
 * Convenience function to paste together multiple columns into one.

 * @param colName Name of the column to add
 * @param sep Separator to use between values.
 * @param remove If TRUE, remove input columns from output data frame.
 *
 * @see @separate
 *
 */
fun DataFrame.unite(colName: String, which: List<String>, sep: String = "_", remove: Boolean = true): DataFrame {
    require(which.isNotEmpty()) { "the column selection to be `unite`ed must not be empty" }

    val uniteBlock = select(which)
    val uniteResult = uniteBlock.rows.map { it.values.map { it?.toString().nullAsNA() }.joinToString(sep) }

    val rest = if (remove) remove(uniteBlock.names) else this

    return rest.addColumn(colName to { uniteResult })
}


fun DataFrame.unite(colName: String, vararg which: ColNames.() -> List<Boolean?>, sep: String = "_", remove: Boolean = true): DataFrame =
        unite(colName, which = colSelectAsNames(reduceColSelectors(which)), sep = sep, remove = remove)


/**
 * Given either regular expression or a vector of character positions, separate() turns a single character column into multiple columns.
 *
 * @param column Bare column name.
 * @param into Names of new variables to create as character vector.
 * @param sep Separator between columns. If character, is interpreted as a regular expression. The default value is a regular @param expression that matches any sequence of non-alphanumeric values. If numeric, interpreted as positions to split at. Positive values start at 1 at the far-left of the string; negative value start at -1 at the far-right of the string. The length of sep should be one less than into.
 * @param remove If TRUE, remove input column from output data frame.
 * @param convert If set, attempt to do a type conversion will be run on all new columns. This                  is useful if the value column was a mix of variables that was coerced to a string.
 */
fun DataFrame.separate(column: String, into: List<String>, sep: String = "_", remove: Boolean = true, convert: Boolean = false): DataFrame {

    val sepCol = this[column]

    // split colum  by given delimter and keep NAs
    val splitData = sepCol.asStrings().map { it?.split(delimiters = sep)?.map { it.naAsNull() } }
    val splitWidths = splitData.map { it?.size }.filterNotNull().distinct()
    val numSplits = splitWidths.first()

    require(splitWidths.size == 1) { "unequal splits are not yet supported" }
    require(numSplits == into.size) { "mimatch between number of splits ${numSplits} and provided new column names '${into}'" }

    // vertically split into columns and perform optional type conversion
    val splitCols: List<DataCol> = (0..(numSplits - 1)).map { splitIndex ->
        println(splitIndex)
        StringCol(into[splitIndex], splitData.map { it?.get(splitIndex) })
    }.map {
        // optionally do type conversion
        if (convert) convertType(it) else it
    }

    // column bind rest and separated columns into final result
    val rest = if (remove) remove(column) else this

    return bindCols(rest, SimpleDataFrame(splitCols))
}