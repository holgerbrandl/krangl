# krangl

[ ![Download](https://api.bintray.com/packages/holgerbrandl/mpicbg-scicomp/krangl/images/download.svg) ](https://bintray.com/holgerbrandl/mpicbg-scicomp/krangl/_latestVersion)  [![Build Status](https://travis-ci.org/holgerbrandl/krangl.svg?branch=master)](https://travis-ci.org/holgerbrandl/krangl) [![Gitter](https://badges.gitter.im/holgerbrandl/krangl.svg)](https://gitter.im/holgerbrandl/krangl?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

`krangl` is a {K}otlin library for data w{rangl}ing. By implementing a grammar of data manipulation using a modern functional-style API, it allows to filter, aggregate and reshape tabular data.

`krangl` is heavily inspired by the amazing [`dplyr`](https://github.com/hadley/dplyr) for [R](https://www.r-project.org/). `krangl` is written in [Kotlin](https://kotlinlang.org/), excels in Kotlin, but emphasizes as well on good java-interop. It is mimicking the API of `dplyr`, while carefully adding more typed constructs where possible.

Installation
------------

To get started simply add it as a dependency via Jcenter:
```
compile "de.mpicbg.scicomp:krangl:0.4"
```

You can also use [JitPack with Maven or Gradle](https://jitpack.io/#holgerbrandl/krangl/-SNAPSHOT) to build the latest snapshot as a dependency in your project.

```groovy
repositories {
    maven { url 'https://jitpack.io' }
}
dependencies {
        compile 'com.github.holgerbrandl:krangl:-SNAPSHOT'
}
```


Examples
--------

```kotlin
// Read data-frame from disk
val iris = DataFrame.fromCSV("data/iris.txt")


// Create data-frame in memory
val df: DataFrame = dataFrameOf(
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
df.addColumn("salary_category") { 3 }

// by doing basic column arithmetics
df.addColumn("age_3y_later") { it["age"] + 3 }

// Note: krangl dataframes are immutable so we need to (re)assign results to preserve changes.
val newDF = df.addColumn("full_name") { it["first_name"] + " " + it["last_name"] }

// Also feel free to mix types here since krangl overloads  arithmetic operators like + for dataframe-columns
df.addColumn("user_id") { it["last_name"] + "_id" + rowNumber }

// Create new attributes with string operations like matching, splitting or extraction.
df.addColumn("with_anz") { it["first_name"].asStrings().map { it!!.contains("anz") } }

// Note: krangl is using 'null' as missing value, and provides convenience methods to process non-NA bits
df.addColumn("first_name_initial") { it["first_name"].asStrings().ignoreNA { first().toString() } }


// Resort with arrange
df.sortedBy("age")
// and add secondary sorting attributes as varargs
df.sortedBy("age", "weight")
df.sortedByDescending("age")
df.sortedBy { it["weight"].asInts() }


// Subset columns with select
df.select2 { it is IntCol } // functional style column selection
df.select("last_name", "weight")    // positive selection
df.remove("weight", "age")  // negative selection
df.select({ endsWith("name") })    // selector mini-language


// Subset rows with vectorized filter
df.filter { it["age"] eq 23 }
df.filter { it["weight"] gt 50 }
df.filter({ it["last_name"].asStrings().map { it!!.startsWith("Do") }.toBooleanArray() })

df.filter({ it["last_name"].asStrings().map { it!!.startsWith("Do") }.toBooleanArray() })

// In case vectorized operations are not possible we can also filter tables by row which allows for scalar operators
df.filterByRow { it["age"] as Int > 5 }


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
sumDF.printDataClassSchema("sumDF")

// This will generate and print the following conversion code:
data class SumDF(val age: Int, val mean_weight: Double, val num_persons: Int)

val records = sumDF.rows.map { row -> SumDF(row["age"] as Int, row["mean_weight"] as Double, row["num_persons"] as Int) }

// Now we can use the krangl result table in a strongly typed way
records.first().mean_weight

// Vice versa we can also convert an existing set of objects into
val dfRestored = records.asDataFrame { mapOf("age" to it.age, "weight" to it.mean_weight) }

```

krangl vs. dplyr
--------------

As said above, `krangl` is mimicking the API of `dplyr`. Here's an example using airline on-time data for all [flights departing NYC in 2013](https://cran.r-project.org/web/packages/nycflights13/index.html).

`dplyr`:
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

And the same rewritten using `krangl`
```{kotlin}
flights
    .groupBy("year", "month", "day")
    .select({ range("year", "day") }, { oneOf("arr_delay", "dep_delay") })
    .summarize(
            "mean_arr_delay" to { it["arr_delay"].mean(removeNA = true) },
            "mean_dep_delay" to { it["dep_delay"].mean(removeNA = true) }
    )
    .filter { (it["mean_arr_delay"] gt  30)  OR  (it["mean_dep_delay"] gt  30) }
```
The biggest different are the comparison operators, which Kotlin does not allow to [be overridden](https://kotlinlang.org/docs/reference/operator-overloading.html) in a vectorized way.

For sure `dplyr` goes way beyond over what is possible with `krangl` at the moment (e.g. database access, 10x better performance). Also other R packages crucial for data science are not yet available in Kotlin. We aim to provide at least few of them as detailed out in our roadmap.

Support & Documentation
----------------------

`krangl` is not yet mature, full of bugs and its API is in constant flux. Nevertheless, feel welcome to submit pull-requests or tickets, or simply get in touch via gitter (see button on top).


* [Krangl Introduction](http://holgerbrandl.github.io/krangl/krangl_intro/krangl_intro.html) A presentation from June 2016 ([sources](./docs/bier_slides_june2016/krangl_intro.md))
* [Krangl User Guide](./docs/user_guide.md) for detailed information about the API and usage examples.
* [Developer Information](./docs/devel.md) with details about to build, test, release and improve `krangl`
* [Roadmap](./docs/roadmap.md) complementing the tracker with where the project is heading

* TBD `krangl` Cheat Sheet





- [X]

Other Related Projects:
* [Joinery](https://github.com/cardillo/joinery): Data frames for Java
* [vectorz](https://github.com/mikera/vectorz): Fast and flexible numerical library for Java featuring N-dimensional arrays
* [golem](https://github.com/kyonifer/golem): A scientific library for Kotlin.
* [Scala DataTable](https://github.com/martincooper/scala-datatable): a lightweight, in-memory table structure written in Scala
* [agate](https://github.com/wireservice/agate): A Python data analysis library that is optimized for humans instead of machines
* [Pandas cheat sheet](https://drive.google.com/folderview?id=0ByIrJAE4KMTtaGhRcXkxNHhmY2M&usp=sharing)
* [koma](https://kyonifer.github.io/koma/) is a scientific computing library written in Kotlin, designed to allow development of cross-platform numerical applications
* [termsql](https://github.com/tobimensch/termsql) converts text from a file or from stdin into SQL table and query it instantly. Uses sqlite as backend.

* https://github.com/netzwerg/paleo which provides immutable Java 8 data frames with typed columns

Data Viz
* https://github.com/timmolter/XChart is a light weight Java library for plotting data
* https://github.com/edvin/tornadofx-guide/blob/master/part1/8.%20Charts.md






`dplyr` references
* [dplyr at CRAN](https://cran.r-project.org/web/packages/dplyr/index.html): Official dplyr website
* [dplyr API docs](http://www.rdocumentation.org/packages/dplyr/functions/dplyr): Online dplyr API docs
