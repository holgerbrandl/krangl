package kutils.kplyr


fun main(args: Array<String>) {
    val df = SimpleDataFrame(DoubleCol("test", listOf(2.toDouble(), 3.toDouble(), 1.toDouble()).toDoubleArray()))

    var newDf = df.mutate("new_attr", { it["test"] + it["test"] })
    // also support varargs with
//    var newDf = df.mutate({"new_attr" to  ( it["test"] + it["test"] )})

    // this works
//    val function: (df:DataFrame) -> List<String> = { listOf<String>("test") }
//    newDf = newDf.mutate("category", function)

    newDf = newDf.mutate("category", { "test" })
//    newDf.print()
    newDf.glimpse()

    newDf = newDf.arrange("new_attr")

    newDf.print()

    newDf = newDf.filter({ it["test"] eq 1 })


//            .groupBy().mutate(it.grp.)

    val groupedDf = newDf.groupBy("new_attr")

    groupedDf.arrange("test").summarize("mean_val", { (it["test"] as DoubleCol).values.sum() })

//      var df = DataFrame.fromTsv()
//    var df = DataFrame(listOf(*cols, DataCol(name, rows.map { formula(it) })))
//
//    val df2 = df.mutate("sdf", {it["sdfd"] + it["sdfdf"]);
//    (("x"->).arrange(desc("baum"))

}

