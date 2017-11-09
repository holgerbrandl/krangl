//@file:Suppress("unused")

package krangl

data class ColumnSelection(val colNames: List<String>)

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


    // unify examples by using sample annotations
    // Core Manipulation Verbs


    /** Keep only the selected columns.
     *
     *  Example

    ```
    df.select( "foo", "bar")
    ```
     */
    fun selectByName(vararg columns: String): DataFrame = selectByName(columns.asList())


    fun select(colSelector: (DataCol) -> Boolean) = selectByName(cols.filter(colSelector).map { it.name })


    /** Convenience wrapper around to work with varag <code>krangl.DataFrame.select</code> */
    fun selectByName(columns: List<String>): DataFrame = selectByName(*columns.toTypedArray())

    /** Keeps only the variables that match any of the given expressions. E.g. use `startsWith("foo")` to select for columns staring with 'foo'.*/
    fun selectByName(which: ColNames.() -> List<Boolean?>): DataFrame = selectByName(*arrayOf(which))

    fun selectByName(vararg which: ColNames.() -> List<Boolean?>): DataFrame {
        val reducedSelector = reduceColSelectors(which)

        return select(reducedSelector)
    }


    // todo consider to use List<Boolean> in signature. We can not provide both because of type erasure
    fun filter(predicate: DataFrame.(DataFrame) -> BooleanArray): DataFrame

    /** Adds new variables and preserves existing.*/
    fun createColumn(tf: ColumnFormula): DataFrame
    // todo maybe as would be more readible: df.mutate({ mean(it["foo")} as "mean_foo")
    // todo Also support varargs similar to summarize: var newDf = df.mutate({"new_attr" to  ( it["test"] + it["test"] )})


    fun createColumn(columnName: String, expression: TableExpression): DataFrame = createColumn(columnName to expression)

    fun createColumns(vararg columSpecs: ColumnFormula): DataFrame = columSpecs.fold(this, { df, tf -> df.createColumn(tf) })

    /** Create a new dataframe based ona a list of column-formulas which are evaluated in the context of the this instance. */
    fun transmute(vararg formula: ColumnFormula) = createColumns(*formula).selectByName(*formula.map { it.name }.toTypedArray())


    // todo also support mini-lang in arrange(); eg: df.arrange(desc("foo"))
    /** Returns a sorted data-frame resorts tables. The first argument defines the primary attribute to sort by. Additional ones are used to
     * resolve ties.
     */
    // inspired by listOf(1,2,3).sortedBy{ it} and  listOf(1,2,3).sortedByDescending{ it}
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




