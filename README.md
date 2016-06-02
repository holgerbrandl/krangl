# kplyr

[![Gitter](https://badges.gitter.im/holgerbrandl/kplyr.svg)](https://gitter.im/holgerbrandl/kplyr?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge) [![Build Status](https://travis-ci.org/holgerbrandl/kplyr.svg?branch=master)](https://travis-ci.org/holgerbrandl/kplyr)

`kplyr` allows to manipulate and integrate tabular data.

`kplyr` is a grammar of data manipulation and a blunt clone of the amazing [`dplyr`](https://github.com/hadley/dplyr) for [R](https://www.r-project.org/). `kplyr` is written in [Kotlin](https://kotlinlang.org/), excels in Kotlin, but emphasizes as well on good java-interop. It is mimicking the API of `dplyr`, while carefully adding more typed constructs where possible.

Installation
------------

To get started simply add it as a dependency via BinTray:
```
compile "de.mpicbg.scicomp:kplyr:0.1-SNAPSHOT"
```




Examples
--------

```kotlin
import kplyr.*


fun main(args: Array<String>) {

    // Create data-frame in memory
    var df: DataFrame = SimpleDataFrame(
            StringCol("first_name", listOf("Max", "Franz", "Horst")),
            StringCol("last_name", listOf("Doe", "Smith", "Keanes")),
            IntCol("age", listOf(23, 23, 12)),
            IntCol("weight", listOf(55, 88, 82))
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
            "mean_weight" to { it["weight"].mean(remNA = true) },
            "num_persons" to { nrow }
    )

    // Optionally ungroup the data
    println("summary is:")
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
```

kplyr vs. dplyr
--------------

As said above, kplyr is mimicking the API of dplyr. Here's an example using airline on-time data for all [flights departing NYC in 2013](https://cran.r-project.org/web/packages/nycflights13/index.html).

dplyr:
```{r}
flights %>%
    group_by(year, month, day) %>%
    select(year:day, arr_delay, dep_delay) %>%
    summarise(
        mean_arr_delay = mean(arr_delay, na.rm = TRUE),
        mean_dep_delay = mean(dep_delay, na.rm = TRUE)
    ) %>%
    filter(mean_arr_delay > 30 | mean_dep_delay > 30)
```

And the same when using `kplyr`
```
flights
    .groupBy("year", "month", "day")
    .select({ range("year", "day") }, { oneOf("arr_delay", "dep_delay") })
    .summarize(
            "mean_arr_delay" to { it["arr_delay"].mean(removeNA = true) },
            "mean_dep_delay" to { it["arr_delay"].mean(removeNA = true) }
    )
    .filter { (it["mean_arr_delay"] gt  30)  OR  (it["mean_arr_delay"] gt  30) }
```
The only major annoyence/limiation are the comparison operators, which Kotlin does not allow to [be overridden](https://kotlinlang.org/docs/reference/operator-overloading.html) in a vectorized way.

For sure `dplyr` goes way beyhond what `kplyr` does at the moment (e.g. database access). Also other R packages crucial for data science are not yet available in Kotlin. We aim to provide at least few of them as detailed out in our roadmap.

Support & Documentation
----------------------

`kplyr` is not yet mature, full of bugs and its API is in constant flux. Nevertheless, feel welcome to submit pull-requests or tickets, or simply get in touch via gitter (see button on top).

* TBD `kplyr` Cheat Sheet

* [Kplyr Introduction](TODO) A presentation from June 2016 ([sources](./docs/bier_slides_june2016/kplyr_intro.md))
* [Kplyr User Guide](./docs/user_guide.md) for detailed information about the API and usage examples.
* [Developer Information](./docs/devel.md) with details about to build, test, release and improve `kplyr`
* [Roadmap](./docs/roadmap.md) complementing the tracker with where the project is heading


References & Related Projects
----------

`kplyr` was inspired by
* [dplyr at CRAN](https://cran.r-project.org/web/packages/dplyr/index.html): Official dplyr website
* [dplyr API docs](http://www.rdocumentation.org/packages/dplyr/functions/dplyr): Online dplyr API docs
* [Pandas cheat sheet](https://drive.google.com/folderview?id=0ByIrJAE4KMTtaGhRcXkxNHhmY2M&usp=sharing)

Related Projects:
* https://github.com/mikera/vectorz: Fast and flexible numerical library for Java featuring N-dimensional arrays
* https://github.com/kyonifer/golem: A scientific library for Kotlin.
* https://github.com/wireservice/agate: A Python data analysis library that is optimized for humans instead of machines