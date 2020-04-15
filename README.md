# krangl

[ ![Download](https://api.bintray.com/packages/holgerbrandl/mpicbg-scicomp/krangl/images/download.svg) ](https://bintray.com/holgerbrandl/mpicbg-scicomp/krangl/_latestVersion)  [![Build Status](https://travis-ci.org/holgerbrandl/krangl.svg?branch=master)](https://travis-ci.org/holgerbrandl/krangl) [![Gitter](https://badges.gitter.im/holgerbrandl/krangl.svg)](https://gitter.im/holgerbrandl/krangl?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

`krangl` is a {K}otlin library for data w{rangl}ing. By implementing a grammar of data manipulation using a modern functional-style API, it allows to filter, transform, aggregate and reshape tabular data.

`krangl` is heavily inspired by the amazing [`dplyr`](https://github.com/hadley/dplyr) for [R](https://www.r-project.org/). `krangl` is written in [Kotlin](https://kotlinlang.org/), excels in Kotlin, but emphasizes as well on good java-interop. It is mimicking the API of `dplyr`, while carefully adding more typed constructs where possible.



[TOC levels=2,2]: # " "

- [Installation](#installation)
- [Features](#features)
- [Examples](#examples)
- [Documentation](#documentation)
- [How to contribute?](#how-to-contribute)


If you're not sure about how to proceed, check out  [krangl in 10 minutes](https://krangl.gitbook.io/docs/getting-started/10_minutes) section in the
**[krangl user guide](https://krangl.gitbook.io/docs/)**.


Installation
------------

To get started simply add it as a dependency via Jcenter:
```
compile "de.mpicbg.scicomp:krangl:0.11"
```

You can also use [JitPack with Maven or Gradle](https://jitpack.io/#holgerbrandl/krangl) to build the latest snapshot as a dependency in your project.

```groovy
repositories {
    maven { url 'https://jitpack.io' }
}
dependencies {
        compile 'com.github.holgerbrandl:krangl:-SNAPSHOT'
}
```

To build and install it into your local maven cache, simply clone the repo and run
```bash
./gradlew install
```


Features
--------

* Filter, transform, aggregate and reshape tabular data
* Modern, user-friendly and easy-to-learn data-science API
* Reads from plain and compressed tsv, csv, json, or any delimited format with or without header from local or remote
* Supports grouped operations
* Ships with JDBC support
* Tables can contain atomic columns (int, double, boolean) as well as object columns
* Reshape tables from wide to long and back
* Table joins (left, right, semi, inner, outer)
* Cross tabulation
* Descriptive statistics (mean, min, max, median, ...)
* Functional API inspired by [dplyr](http://dplyr.tidyverse.org/), [pandas](http://pandas.pydata.org/), and Kotlin [stdlib](https://kotlinlang.org/api/latest/jvm/stdlib/index.html)

* many more...

`krangl` is _just_ about data wrangling. For data visualization we recommend [`kravis`](https://github.com/holgerbrandl/kravis) which seamlessly integrates with krangl and implements a grammar to build a wide variety of plots.


Examples
--------

```kotlin
// Read data-frame from disk
val iris = DataFrame.readTSV("data/iris.txt")


// Create data-frame in memory
val df: DataFrame = dataFrameOf(
    "first_name", "last_name", "age", "weight")(
    "Max", "Doe", 23, 55,
    "Franz", "Smith", 23, 88,
    "Horst", "Keanes", 12, 82
)

// Or from csv
// val otherDF = DataFrame.readCSV("path/to/file")

// Print rows
df                              // with implict string conversion using default options
df.print(colNames = false)      // with custom printing options

// Print structure
df.schema()


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
df.addColumn("first_name_initial") { it["first_name"].map<String>{ it.first() } }

// or add multiple columns at once
df.addColumns(
    "age_plus3" to { it["age"] + 3 },
    "initials" to { it["first_name"].map<String> { it.first() } concat it["last_name"].map<String> { it.first() } }
)


// Sort your data with sortedBy
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
df.filter({ it["last_name"].isMatching { startsWith("Do")  }})

// In case vectorized operations are not possible or available we can also filter tables by row
// which allows for scalar operators
df.filterByRow { it["age"] as Int > 5 }
df.filterByRow { (it["age"] as Int).rem(10) == 0 } // round birthdays :-)


// Summarize

// do simple cross tabulations
df.count("age", "last_name")

// ... or calculate single summary statistic
df.summarize("mean_age" to { it["age"].mean(true) })

// ... or multiple summary statistics
df.summarize(
    "min_age" to { it["age"].min() },
    "max_age" to { it["age"].max() }
)

// for sake of r and python adoptability you can also use `=` here
df.summarize(
    "min_age" `=` { it["age"].min() },
    "max_age" `=` { it["age"].max() }
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
sumDF.printDataClassSchema("Person")

// This will generate and print the following conversion code:
data class Person(val age: Int, val mean_weight: Double, val num_persons: Int)

val records = sumDF.rows.map { row -> Person(row["age"] as Int, row["mean_weight"] as Double, row["num_persons"] as Int) }

// Now we can use the krangl result table in a strongly typed way
records.first().mean_weight

// Vice versa we can also convert an existing set of objects into
val recordsDF = records.asDataFrame()
recordsDF.print()

// to populate a data-frame with selected properties only, we can do
val deparsedDF = records.deparseRecords { mapOf("age" to it.age, "weight" to it.mean_weight) }

```

Documentation
-------------

`krangl` is not yet mature, full of bugs and its API is in constant flux. Nevertheless, feel welcome to submit pull-requests or tickets, or simply get in touch via gitter (see button on top).

* [Krangl User Guide](https://krangl.gitbook.io/docs/) for detailed information about the API and usage examples.
* [API Docs](http://holgerbrandl.github.io/krangl/javadoc/krangl/) for detailed information about the API including manu usage examples
* TBD `krangl` Cheat Sheet


Another great [introduction into data-science with kotlin](https://blog.jetbrains.com/kotlin/2019/12/making-kotlin-ready-for-data-science/)
 was presented at 2019's [KotlinConf](https://kotlinconf.com/)
  by Roman Belov from [JetBrains](https://www.jetbrains.com/).



How to contribute?
------------------

Feel welcome to post ideas, suggestions and criticism to our [tracker](https://github.com/holgerbrandl/krangl/issues).

We always welcome pull requests. :-)

You could also show your spiritual support by upvoting `krangl` here on github.

Also see

* [Developer Information](./docs/devel.md) with technical notes & details about to build, test, release and improve `krangl`
* [Roadmap](./docs/roadmap.md) complementing the tracker with a backlog

Also, there are a few issues in the IDE itself which limit the applicability/usability of `krangl`,  So, you may want to vote for

* [KT-23526](https://youtrack.jetbrains.com/issue/KT-11473) In *.kts scripts, debugger ignores breakpoints in top-level statements and members
* [KT-24789](https://youtrack.jetbrains.net/issue/KT-24789) "Unresolved reference" when running a script which is a symlink to a script outside of source roots
* [KT-12583](https://youtrack.jetbrains.com/issue/KT-12583) IDE REPL should run in project root directory
* [KT-11409](https://youtrack.jetbrains.com/issue/KT-11409) Allow to "Send Selection To Kotlin Console"
* [KT-13319](https://youtrack.jetbrains.net/issue/KT-13319) Support ":paste" for pasting multi-line expressions in REPL
* [KT-21224](https://youtrack.jetbrains.net/issue/KT-21224) REPL output is not aligned with input



# References

Similar APIs (not just Kotlin)
* [Scala DataTable](https://github.com/martincooper/scala-datatable): a lightweight, in-memory table structure written in Scala
* [joinery](https://github.com/cardillo/joinery) implements data frames for Java
* [tablesaw](https://github.com/jtablesaw/tablesaw) which is (according to its authors) the _The simplest way to slice data in Java_
* [paleo](https://github.com/netzwerg/paleo) which provides immutable Java 8 data frames with typed columns
* [agate](https://github.com/wireservice/agate) isa  Python data analysis library that is optimized for humans instead of machines
* [pandas](http://pandas.pydata.org/) provides high-performance, easy-to-use data structures and data analysis tools for python ([cheatsheet](https://drive.google.com/folderview?id=0ByIrJAE4KMTtaGhRcXkxNHhmY2M&usp=sharing))
* [dplyr](https://cran.r-project.org/web/packages/dplyr/index.html) which is a grammar of data manipulation (R-lang)
* [morpheus-core](https://github.com/zavtech/morpheus-core) which is a data science framework implementing an R-like data-frame for the JVM
* [Frameless](https://github.com/typelevel/frameless) is a Scala library for working with Spark using more expressive types, including a more strongly typed Dataset/[DataFrame API](https://typelevel.org/frameless/TypedDataFrame.html)




Other data-science projects:
* [vectorz](https://github.com/mikera/vectorz) is a fast and flexible numerical library for Java featuring N-dimensional arrays
* [koma](https://kyonifer.github.io/koma/) is a scientific computing library written in Kotlin, designed to allow development of cross-platform numerical applications
* [termsql](https://github.com/tobimensch/termsql) converts text from a file or from stdin into SQL table and query it instantly. Uses sqlite as backend.
* [kotliquery](https://github.com/seratch/kotliquery) is a handy database access library
* [Dex : The Data Explorer](https://github.com/PatMartin/Dex) is a data visualization tool written capable of powerful ETL and publishing web visualizations

Data Visualization
* [kravis](https://github.com/holgerbrandl/kravis) which implements a Kotlin DSL for scientific data visualization
* [XChart](https://github.com/timmolter/XChart) is a light weight Java library for plotting data
* [Charts in TornadoFX](https://github.com/edvin/tornadofx-guide/blob/master/part1/8.%20Charts.md) provide visualzation examples using javaFX

