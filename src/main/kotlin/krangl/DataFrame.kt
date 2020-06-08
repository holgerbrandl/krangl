package krangl


typealias DataFrameRow = Map<String, Any?>

typealias VectorizedRowPredicate = ExpressionContext.(ExpressionContext) -> BooleanArray

typealias TableExpression = ExpressionContext.(ExpressionContext) -> Any?


interface DataFrame {

    // Accessor functions

    /** @return Number of rows in this dataframe. */
    val nrow: Int

    /** @return Number of columns in this dataframe. */
    val ncol: Int

    /** Returns the ordered list of column names of this data-frame. */
    val names: List<String>


    /** Returns the ordered list of column this data-frame. */
    val cols: List<DataCol>

    /** An iterator over the row numbers in the data-frame. */
    //    val rowNumber: Iterable<Int>


    /** Returns a column by name. */
    operator fun get(columnName: String): DataCol

    /** Returns a column by index. */
    operator fun get(columnIndex: Int): DataCol = get(names[columnIndex])

    ////    /** Assign a value to a column */
    //    operator fun set(columnName: String, value: Any) {
    //
    //    }
    ////     disabled because would render dfs mutable, also it is not compatible with

    // should we use invoke() style operator here (see https://kotlinlang.org/docs/reference/operator-overloading.html)?
    /** Returns a row by index **/
    fun row(rowIndex: Int): DataFrameRow


    fun filterByRow(rowFilter: DataFrameRow.(DataFrameRow) -> Boolean): DataFrame {
        val filterIndex = this.rows.map { it.rowFilter(it) }
        return filter { filterIndex.toBooleanArray() }
    }


    /** @return An iterator over all rows. Per row data is represented as a named map.

    Example:

    ```
    df.rows.map { row -> SumDF(row["age"] as Int, row["mean_weight"] as Double, row["num_persons"] as Int) }
    ```
     */
    val rows: Iterable<DataFrameRow>


    /** Select or remove columns by predicate.
     *
     * @sample krangl.samples.selectExamples
     */
    fun selectIf(colSelector: (DataCol) -> Boolean) = select(cols.filter(colSelector).map { it.name })


    /** Select or remove columns by predicate.
     *
     * @sample krangl.samples.selectExamples
     */
    fun removeIf(colSelector: (DataCol) -> Boolean) = select(cols.filterNot(colSelector).map { it.name })

    // unify examples by using sample annotations
    // Core Manipulation Verbs


    /** Create a new data frame with only the selected columns.
     *
     * @sample krangl.samples.selectExamples
     */
    fun select(vararg columns: String): DataFrame

    /** Remove selected columns.
     *
     * @sample krangl.samples.selectExamples
     */
    fun remove(vararg columns: String): DataFrame = select(names.minus(columns.asList()))

    /** Convenience wrapper around to work with varag <code>krangl.DataFrame.select</code> */
    fun select(columns: Iterable<String>): DataFrame = select(*columns.toList().toTypedArray())

    /** Remove selected columns.
     *
     * @sample krangl.samples.selectExamples
     */
    fun remove(columns: Iterable<String>): DataFrame = remove(*columns.toList().toTypedArray())

    /** Keeps only the variables that match any of the given expressions.
     *
     * E.g. use `startsWith("foo")` to select for
     * columnSelect staring with 'foo'.
     *
     * @sample krangl.samples.selectExamples
     */
    fun select(columnSelect: ColumnSelector): DataFrame = select(colSelectAsNames(columnSelect))


    // `drop` as method name is burnt here since kotlin stdlin contains it for collections.
    /** Remove selected columns using a predicate
     *
     * @sample krangl.samples.selectExamples
     */
    fun remove(columnSelect: ColumnSelector): DataFrame = select { except(columnSelect) }


    /** Create a new data frame with only the selected columns.
     *
     * @sample krangl.samples.selectExamples
     */
    fun select(vararg columns: ColumnSelector): DataFrame {
        val reducedSelector = reduceColSelectors(columns)

        return select(reducedSelector)
    }

    /** Remove selected columns.
     *
     * @sample krangl.samples.selectExamples
     */
    fun remove(vararg columSelects: ColumnSelector): DataFrame =
            select(*columSelects.map { it -> { x: ColNames -> x.except(it) } }.toTypedArray())


    fun filter(predicate: VectorizedRowPredicate): DataFrame

    /** Adds new variables and preserves existing.*/
    fun addColumn(tf: ColumnFormula): DataFrame

    fun addColumn(columnName: String, expression: TableExpression): DataFrame = addColumn(columnName to expression)

    fun addColumns(vararg columnFormulas: ColumnFormula): DataFrame = columnFormulas.fold(this, { df, tf -> df.addColumn(tf) })

    /** Create a new dataframe based on a list of column-formulas which are evaluated in the context of the this instance. */
    fun transmute(vararg formula: ColumnFormula) = addColumns(*formula).select(*formula.map { it.name }.toTypedArray())


    /** Resorts the receiver in ascending order (small values to go top of table). The first argument defines the
     * primary attribute to sort by. Additional ones are used to resolve ties.
     *
     * Missing values will come last in the sorted table.
     *
     * Works similar as  `listOf(1,2,3).sortedBy{ it }`
     */
    fun sortedBy(vararg by: String): DataFrame


    /** Resorts the receiver in descending order (small values to go bottom of table). The first argument defines the
     * primary attribute to sort by. Additional ones are used to resolve ties.
     *
     * Works similar as `listOf(1,2,3).sortedByDescending{ it }`
     */
    fun sortedByDescending(vararg by: String): DataFrame {
        return by.map { sorter ->
            val sortExpression: SortExpression = { desc(sorter) }
            sortExpression
        }.let {
            sortedBy(*it.toTypedArray())
        }
    }


    /**
     * Creates a summary of a table or a group. The provided expression is expected to evaluate to a scalar value and not into a column.
     * `summarise()` is typically used on grouped data created by group_by(). The output will have one row for each group.
     */
    fun summarize(vararg sumRules: ColumnFormula): DataFrame


    // Grouping

    /**
     * Creates a grouped data-frame given a list of grouping attributes.
     *
     * Most data operations are done on groups defined by variables. `group_by()` takes the receiver data-frame and
     * converts it into a grouped data-frame where operations are performed "by group". `ungroup()` removes grouping.
     *
     * Most krangl verbs like `addColumn()`, `summarize()`, etc. will be executed per group if a grouping is present.
     *
     * @sample krangl.samples.groupByExamples
     *
     */
    fun groupBy(vararg by: String): DataFrame


    /** Removes the grouping (if present from a data frame. */
    fun ungroup(): DataFrame


    // needed for static extensions (see http://stackoverflow.com/questions/28210188/static-extension-methods-in-kotlin)
    companion object {}

    /** Returns a data-frame of distinct grouping variable tuples for a grouped data-frame. An empty data-frame for ungrouped data.*/
    fun groupedBy(): DataFrame

    /** Returns the groups of a grouped data frame or just a reference to this object if not.*/
    fun groups(): List<DataFrame>
}

