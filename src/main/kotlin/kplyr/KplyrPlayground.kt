package kplyr


fun main(args: Array<String>) {
    // create data-frame in memory
    val df = SimpleDataFrame(DoubleCol("test", listOf(2.toDouble(), 3.toDouble(), 1.toDouble())))

    // or from csv
    val otherDF = fromCSV("path/to/file")

    // print rows
    df                              // with default printing options
    df.print(colNames = false)      // with custom  printing options

    // print structure
    df.glimpse()


    // add columns with mutate
    var mutDf = df.mutate("new_attr", { it["test"] + it["test"] })
    df.mutate("order_name", { "pos" + rowNumber() })
    mutDf = mutDf.mutate("category", { 3 })

    // or access raw column data without extension function for more custom operations

    mutDf.mutate("cust_value", { (it["test"] as DoubleCol).values.first() })


    // resort with arrange
    mutDf = mutDf.arrange("new_attr")


    // subset columns with select
    mutDf.select("test", "new_attr")    // positive selection
    mutDf.select(-"test", -"category")  // negative selection
    mutDf.select({ startsWith("te") })    // selector mini-language


    // subset rows with filter
    mutDf.filter { it["test"] gt 2 }
    mutDf.filter { it["category"] eq "A" }

    // summarize
    mutDf.summarize("mean_test" to { it["new_attr"].max() })
    mutDf.summarize("mean_test" to { it["new_attr"].max() }, "naha" to { it["new_attr"].max() }).print()


    // grouped operations
    val groupedDf: DataFrame = mutDf.groupBy("new_attr", "category")
    groupedDf.summarize("mean_val", { it["test"].mean(remNA = true) })

    val sumDf = groupedDf.ungroup()

    // generate object bindings for kotlin
    sumDf.toKotlin("groupedDF")
}




