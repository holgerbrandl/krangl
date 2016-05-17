# kplyr

[![Gitter](https://badges.gitter.im/holgerbrandl/kplyr.svg)](https://gitter.im/holgerbrandl/kplyr?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

`kplyr` allows to manipulate and integrate tabular data.

`kplyr` is a grammar of data manipulation and a blunt clone of the amazing [`dplyr`](https://github.com/hadley/dplyr) for [R](https://www.r-project.org/). `kplyr` is written in [Kotlin](https://kotlinlang.org/) but emphasizes on good java-interop. It mimicking the API of `dplyr` as much as possible, while carefully adding more typed constructs where possible.

Installation
------------

To get started simply add it as a dependency via BinTray:
```
compile "de.mpicbg.scicomp:kplyr:0.1-SNAPSHOT"
```




Examples
--------

```kotlin
fun main(args : Array<String>) {

    // create data-frame in memory
    val df = SimpleDataFrame(IntCol("test", listOf(1,2,3)))

    // or from csv
    val otherDF = fromCSV("path/to/file")

    // print rows
    df // ..using default printing options
    df.print(colNames = false)

    // print structure
    df.glimpse()


    // add columns with mutate
    var mutDf = df.mutate("new_attr", { it["test"] + it["test"] })
    df.mutate("order_name", { "pos"+ rowNumber() })
    mutDf = mutDf.mutate("category", { 3 })

    // or access raw column data without extension function for more custom operations

    mutDf.mutate("cust_value", { (it["test"] as DoubleCol).values.first() })


    // resort with arrange
    mutDf = mutDf.arrange("new_attr")


    // subset columns with select
    mutDf.select("test", "new_attr")    // positive selection
    mutDf.select(-"test", -"category")  // negative selection
    mutDf.select({startsWith("te")})    // selector mini-language


    // subset rows with filter
    mutDf.filter { it["test"] gt 2 }
    mutDf.filter { it["category"] eq "A" }

    // summarize
    mutDf.summarize("mean_test" to { it["new_attr"].max()})
    mutDf.summarize("mean_test" to { it["new_attr"].max()}, "naha" to { it["new_attr"].max()}).print()


    // grouped operations
    val groupedDf: DataFrame = mutDf.groupBy("new_attr", "category")
    groupedDf.summarize("mean_val", { it["test"].mean(remNA=true)})

    val sumDf = groupedDf.ungroup()

    // generate object bindings for kotlin
    sumDf.toKotlin("groupedDF")
}

```

Support & Documentation
----------------------

`kplyr` not yet mature, full of bugs and its API is in constant flux. Nevertheless, feel welcome to submit pull-requests or tickets, or simply get in touch via gitter (see button on top).

* Cheat Sheet TBD


References
----------

* [dplyr at CRAN](https://cran.r-project.org/web/packages/dplyr/index.html)
* [dplyr API docs](http://www.rdocumentation.org/packages/dplyr/functions/dplyr)