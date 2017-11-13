@file:Suppress("CanBeVal")

package krangl.scratch

import krangl.*


@Suppress("UNUSED_VARIABLE")
fun main(args: Array<String>) {

    val someIris = DataFrame.fromCSV("/Users/brandl/Library/Preferences/IntelliJIdea2017.2/scratches/scratch_52.txt")

    flightsData.print()
    val msleep = DataFrame.fromCSV("/Users/brandl/projects/kotlin/krangl/src/test/resources/krangl/data/msleep.csv")
    msleep.glimpse()
    msleep.print()


    msleep.groupBy("order").summarize(
        "mean_weight" to { it["bodywt"].mean(true) }
    ).filter({ it["mean_weight"] gt 20 }).print()


    //    exitProcess(0)


    // create data-frame in memory
    var df: DataFrame = SimpleDataFrame(
        StringCol("first_name", listOf("Max", "Franz", "Horst")),
        StringCol("last_name", listOf("Doe", "Smith", "Keanes")),
        IntCol("age", listOf(23, 23, 12)),
        IntCol("weight", listOf(55, 88, 82))
    )


    //    df.mutate("user_id", { "id" + rowNumber() }) // broken since it does not expand to column

    // should adding of arrays be supported?
    val foo = df.addColumn("user_id") { const("id") + nrow }

    //    df.mutate("user_id", { const("id") + (1..10) /**/}) // this should not be valid anyway

    //    df.select(-"weight", -"age")  // negative selection
    //
    //    df.toKotlin("df")
    //
    //    // generated code here
    //    data class Df(val first_name: String, val last_name: String, val age: Int, val weight: Int)
    //
    //    val dfEntries = df.rows.map { row -> Df(row["first_name"] as String, row["last_name"] as String, row["age"] as Int, row["weight"] as Int) }


    // or access raw column data without extension function for more custom operations

    //    mutDf.mutate("first_part", { it["test"].asStrings().map { it?.split("_".toRegex(), 2)?.get(2) ?: null } })
    //    mutDf.mutate("first_part", { it["test"].dataNA<String> { split("_".toRegex(), 2).get(2) } })
    //    mutDf.mutate("first_part", { it["test"].dataNA<Int> { +3 } })
    //    mutDf.mutate("first_part", { it["test"].dataNA<Int> { this+3 } })
    //
    // alternative to     mutDf.mutate("first_part", { it["test"].asStrings().map{ it?.split("_".toRegex(), 2)?.get(2) ?: null}})


    //    // grouped operations
    val groupedDf: DataFrame = df.groupBy("age")
    //    groupedDf.summarize("mean_val", { it["test"].mean(remNA = true) })
    //
    //    val sumDf = groupedDf.ungroup()

    // generate object bindings for kotlin
    //    df.toKotlin("groupedDF")
}
