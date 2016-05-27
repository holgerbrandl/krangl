@file:Suppress("unused")

package kplyr

interface DataFrame {

    // Accessor functions

    /** @return Number of rows in this dataframe. */
    val nrow: Int
    /** @return Number of columns in this dataframe. */
    val ncol: Int

    /** Returns the ordered list of column names of this data-frame. */
    val names: List<String>

    operator fun get(name: String): DataCol


    // todo use invoke() style operator here (see https://kotlinlang.org/docs/reference/operator-overloading.html)
    fun row(rowIndex: Int): Map<String, Any?>

    val rows: Iterable<Map<String, Any?>>


    // Core Manipulation Verbs

    /** select() keeps only the variables you mention.*/
    fun select(which: List<Boolean>): DataFrame

    // todo consider to use List<Boolean> in signature. We can not provide both because of type erasure
    fun filter(predicate: DataFrame.(DataFrame) -> BooleanArray): DataFrame

    /** Mutate adds new variables and preserves existing.*/
    fun mutate(tf: TableFormula): DataFrame
    // todo maybe as would be more readible: df.mutate({ mean(it["foo")} as "mean_foo")
    // todo Also support varargs similar to summarize: var newDf = df.mutate({"new_attr" to  ( it["test"] + it["test"] )})


    // todo also support mini-lang in arrange(); eg: df.arrange(desc("foo"))
    /** arrange resorts tables. The first argument defines the primary attribute to sort by. Additional ones are used to
     * resolve ties.
     */
    fun arrange(vararg by: String): DataFrame

    /** Creates a summary of a table or a group. The provided formula is expected to evaluate to a scalar value and not into a column.
     * @throws
     */
    fun summarize(vararg sumRules: TableFormula): DataFrame


    // Grouping

    /** Creates a grouped data-frame given a list of grouping attributes. Most kplyr verbs like mutate, summarize,
     * etc. will be executed per group.
     */
    fun groupBy(vararg by: String): DataFrame

    /** Removes the grouping (if present from a data frame. */
    fun ungroup(): DataFrame

    // needed for static extensions (see http://stackoverflow.com/questions/28210188/static-extension-methods-in-kotlin)
    companion object {}
}


