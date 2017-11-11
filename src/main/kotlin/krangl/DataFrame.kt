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




    operator fun get(name: String): DataCol


    // todo use invoke() style operator here (see https://kotlinlang.org/docs/reference/operator-overloading.html)
    fun row(rowIndex: Int): DataFrameRow


    fun filterByRow(rowFilter: DataFrameRow.(DataFrameRow) -> Boolean) : DataFrame {
        val filterIndex = this.rows.map{ it.rowFilter(it)}
        return filter {  filterIndex.toBooleanArray() }
    }


    /** @return An iterator over all rows. Per row data is represented as a named map.

    Example

    ```
    df.rows.map { row -> SumDF(row["age"] as Int, row["mean_weight"] as Double, row["num_persons"] as Int) }
    ```
     */
    val rows: Iterable<DataFrameRow>

    /** Select or remove columns by predicate.

    Example:

    ```
    foo.select2{ it is IntCol }
    foo.select2{ it.name.startsWith("bar") }

    foo.remove{ it.name.startsWith("bar") }
    ```
     */
    fun select2(colSelector: (DataCol) -> Boolean) = select(cols.filter(colSelector).map { it.name })

    fun remove2(colSelector: (DataCol) -> Boolean) = select(cols.filterNot(colSelector).map { it.name })

    // unify examples by using sample annotations
    // Core Manipulation Verbs


    /** Keep only the selected columns.
     *
     *  Example

    ```
    df.select( "foo", "bar")
    ```
     */
    fun select(vararg columns: String): DataFrame = select(columns.asList())

    fun remove(vararg columns: String): DataFrame = select(names.minus(columns.asList()))

    /** Convenience wrapper around to work with varag <code>krangl.DataFrame.select</code> */
    fun select(columns: Iterable<String>): DataFrame = select(*columns.toList().toTypedArray())

    fun remove(columns: Iterable<String>): DataFrame = remove(*columns.toList().toTypedArray())

    /** Keeps only the variables that match any of the given expressions. E.g. use `startsWith("foo")` to select for columns staring with 'foo'.*/
    fun select(columns: ColumnSelector): DataFrame = select(*arrayOf(columns))

    // `drop` as method name is burnt here since kotlin stdlin contains it for collections.
    fun remove(columnSelect: ColumnSelector): DataFrame = select { except(columnSelect) }


    fun select(vararg columns: ColumnSelector): DataFrame {
        val reducedSelector = reduceColSelectors(columns)

        if(reducedSelector.toList().filterNotNull().distinct().size >1){
           throw InvalidColumnSelectException(names,reducedSelector)
        }

        return select(reducedSelector)
    }

    fun remove(vararg columSelects: ColumnSelector): DataFrame =
            select(*columSelects.map { it -> { x: ColNames -> x.except(it) } }.toTypedArray())


    // todo consider to use List<Boolean> in signature. We can not provide both because of type erasure
    fun filter(predicate: VectorizedRowPredicate): DataFrame

    /** Adds new variables and preserves existing.*/
    fun addColumn(tf: ColumnFormula): DataFrame
    // todo maybe as would be more readible: df.mutate({ mean(it["foo")} as "mean_foo")
    // todo Also support varargs similar to summarize: var newDf = df.mutate({"new_attr" to  ( it["test"] + it["test"] )})


    fun addColumn(columnName: String, expression: TableExpression): DataFrame = addColumn(columnName to expression)

    fun addColumns(vararg columnFormulas: ColumnFormula): DataFrame = columnFormulas.fold(this, { df, tf -> df.addColumn(tf) })

    /** Create a new dataframe based ona a list of column-formulas which are evaluated in the context of the this instance. */
    fun transmute(vararg formula: ColumnFormula) = addColumns(*formula).select(*formula.map { it.name }.toTypedArray())


    /** Returns a sorted data-frame resorts tables. The first argument defines the primary attribute to sort by. Additional ones are used to
     * resolve ties.
     * Works similar as  `listOf(1,2,3).sortedBy{ it }` and  `listOf(1,2,3).sortedByDescending{ it }`
     */
    fun sortedBy(vararg by: String): DataFrame

    fun sortedByDescending(vararg by: String): DataFrame = TODO()

    /** Creates a summary of a table or a group. The provided expression is expected to evaluate to a scalar value and not into a column.
     * @throws
     */
    fun summarize(vararg sumRules: ColumnFormula): DataFrame


    // Grouping

    /** Creates a grouped data-frame given a list of grouping attributes. Most krangl verbs like mutate, summarize,
     * etc. will be executed per group.
     */
    fun groupBy(vararg by: String): DataFrame

    /** Removes the grouping (if present from a data frame. */
    fun ungroup(): DataFrame

    // needed for static extensions (see http://stackoverflow.com/questions/28210188/static-extension-methods-in-kotlin)
    companion object {}
}




