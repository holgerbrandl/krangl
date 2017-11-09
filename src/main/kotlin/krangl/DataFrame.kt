//@file:Suppress("unused")

package krangl

internal data class ColumnSelection(val colNames: List<String>)

typealias ColumnSelector = ColNames.() -> List<Boolean?>

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
    val rowNumber: Iterable<Int>


    operator fun get(name: String): DataCol


    // todo use invoke() style operator here (see https://kotlinlang.org/docs/reference/operator-overloading.html)
    fun row(rowIndex: Int): Map<String, Any?>

    /** @return An iterator over all rows. Per row data is represented as a named map.

    Example

    ```
    df.rows.map { row -> SumDF(row["age"] as Int, row["mean_weight"] as Double, row["num_persons"] as Int) }
    ```
     */
    val rows: Iterable<Map<String, Any?>>


    fun select2(colSelector: (DataCol) -> Boolean) = select(cols.filter(colSelector).map { it.name })

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
    fun select(columns: List<String>): DataFrame = select(*columns.toTypedArray())

    fun remove(columns: List<String>): DataFrame = remove(*columns.toTypedArray())

    /** Keeps only the variables that match any of the given expressions. E.g. use `startsWith("foo")` to select for columns staring with 'foo'.*/
    fun select(columns: ColumnSelector): DataFrame = select(*arrayOf(columns))

    // `drop` as method name is burnt here since kotlin stdlin contains it for collections.
    fun remove(columSelect: ColumnSelector): DataFrame = select { without(columSelect) }


    fun select(vararg columns: ColumnSelector): DataFrame {
        val reducedSelector = reduceColSelectors(columns)

        return select(reducedSelector)
    }

    fun remove(vararg columSelects: ColumnSelector): DataFrame =
            select(*columSelects.map { it -> { x: ColNames -> x.without(it) } }.toTypedArray())


    // todo consider to use List<Boolean> in signature. We can not provide both because of type erasure
    fun filter(predicate: DataFrame.(DataFrame) -> BooleanArray): DataFrame

    /** Adds new variables and preserves existing.*/
    fun addColumn(tf: ColumnFormula): DataFrame
    // todo maybe as would be more readible: df.mutate({ mean(it["foo")} as "mean_foo")
    // todo Also support varargs similar to summarize: var newDf = df.mutate({"new_attr" to  ( it["test"] + it["test"] )})


    fun addColumn(columnName: String, expression: TableExpression): DataFrame = addColumn(columnName to expression)

    fun addColumns(vararg columSpecs: ColumnFormula): DataFrame = columSpecs.fold(this, { df, tf -> df.addColumn(tf) })

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




