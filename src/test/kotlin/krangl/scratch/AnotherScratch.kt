package krangl.scratch

import krangl.*

object Foo {
    @JvmStatic
    fun main(args: Array<String>) {
        val mpg_df = DataFrame.readCSV("https://jetbrains.bintray.com/lets-plot/mpg.csv")

        val dat = (mpg_df.names.filter { it.isNotBlank() }.map { Pair(it, mpg_df.get(it).values()) }).toMap()

        mpg_df.print()
//        println(dat.to)
        mpg_df.asMap()

        val foo = dataFrameOf("", "str")(
            1, 2
        )

        foo.print()

        val person1 = mapOf("person" to "James", "year" to 1996)
        val person2 = mapOf("person" to "Anne", "year" to 1998)

        emptyDataFrame().bindRows(person1, person2).print()


    }

    fun DataFrame.asMap() = names.filter { it.isNotBlank() }.map { Pair(it, get(it).values()) }.toMap()
}


object Bar {
    @JvmStatic
    fun main(args: Array<String>) {

        // type conversion with current API
        irisData
            .addColumn("str") { "3" }
            .addColumn("int") { it["str"].map<String> { it.toInt() } }
            .schema()


        // with extension (could/should we add this to API?)
        fun DataCol.toInt(): List<Any?> = map<String> { it.toInt() }

        irisData
            .addColumn("str") { "3" }
            .addColumn("int") { it["str"].toInt() }
            .schema()

        // implicit conversions        
        irisData
            .addColumn("str") { "3" }
            .addColumn("int") { it["str"].toInt() }

            // supported already
            .addColumn("impl_3") { it["str"] + it["int"] }


            // not supported but should be
            .addColumn("impl_1") { it["str"] + 3 }
            .addColumn("impl_3") { it["int"] + it["str"] }
            .addColumn("impl_2") { 3 + it["str"] }
            .schema()
    }
}

private operator fun Int.plus(dataCol: DataCol) {
    TODO("Not yet implemented")
}
