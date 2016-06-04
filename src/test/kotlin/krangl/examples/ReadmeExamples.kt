package krangl.examples

import krangl.*


fun main(args: Array<String>) {

    // Create data-frame in memory

    var df: DataFrame = dataFrameOf(
            "first_name", "last_name", "age", "weight")(
            "Max", "Doe", 23, 55,
            "Franz", "Smith", 23, 88,
            "Horst", "Keanes", 12, 82
    )

    // Or from csv
    // val otherDF = fromCSV("path/to/file")

    // Print rows
    df                              // with default printing options
    df.print(colNames = false)      // with custom  printing options

    // Print structure
    df.glimpse()


    // Add columns with mutate
    // by adding constant values as new column
    df.mutate("salary_category", { 3 })

    // by doing basic column arithmetics
    df.mutate("age_3y_later", { it["age"] + 3 })

    // Note: kplyr dataframes are immutable so we need to (re)assign results to preserve changes.
    df = df.mutate("full_name", { it["first_name"] + " " + it["last_name"] })

    // Also feel free to mix types here since kplyr overloads  arithmetic operators like + for dataframe-columns
    df.mutate("user_id", { it["last_name"] + "_id" + rowNumber() })

    // Create new attributes with string operations like matching, splitting or extraction.
    df.mutate("with_anz", { it["first_name"].asStrings().map { it!!.contains("anz") } })

    // Note: kplyr is using 'null' as missing value, and provides convenience methods to process non-NA bits
    df.mutate("first_name_restored", { it["full_name"].asStrings().ignoreNA { split(" ".toRegex(), 2)[1] } })


    // Resort with arrange
    df.arrange("age")
    // and add secondary sorting attributes as varargs
    df.arrange("age", "weight")


    // Subset columns with select
    df.select("last_name", "weight")    // positive selection
    df.select(-"weight", -"age")  // negative selection
    df.select({ endsWith("name") })    // selector mini-language


    // Subset rows with filter
    df.filter { it["age"] eq 23 }
    df.filter { it["weight"] gt 50 }
    df.filter({ it["last_name"].asStrings().map { it!!.startsWith("Do") }.toBooleanArray() })


    // Summarize
    // ... single summary statistic
    df.summarize("mean_age" to { it["age"].mean(true) })
    // ... multiple summary statistics
    df.summarize(
            "min_age" to { it["age"].min() },
            "max_age" to { it["age"].max() }
    )


    // Grouped operations
    val groupedDf: DataFrame = df.groupBy("age") // or provide multiple grouping attributes with varargs
    val sumDF = groupedDf.summarize(
            "mean_weight" to { it["weight"].mean(removeNA = true) },
            "num_persons" to { nrow }
    )

    // Optionally ungroup the data
    sumDF.ungroup().print()

    // generate object bindings for kotlin.
    // Unfortunately the syntax is a bit odd since we can not access the variable name by reflection
    sumDF.toKotlin("sumDF")
    // This will generate and print the following conversion code:
    data class SumDF(val age: Int, val mean_weight: Double, val num_persons: Int)

    val sumDFEntries = sumDF.rows.map { row -> SumDF(row["age"] as Int, row["mean_weight"] as Double, row["num_persons"] as Int) }

    // Now we can use the kplyr result table in a strongly typed way
    sumDFEntries.first().mean_weight
}
