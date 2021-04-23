# krangl

[ ![Download](https://img.shields.io/badge/Maven%20Central-0.16.1-orange) ](https://mvnrepository.com/artifact/com.github.holgerbrandl/krangl)  [![Build Status](https://github.com/holgerbrandl/krangl/workflows/build/badge.svg)](https://github.com/holgerbrandl/krangl/actions?query=workflow%3Abuild) [![Gitter](https://badges.gitter.im/holgerbrandl/krangl.svg)](https://gitter.im/holgerbrandl/krangl?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

`krangl` is a {K}otlin library for data w{rangl}ing. By implementing a grammar of data manipulation using a modern functional-style API, it allows to filter, transform, aggregate and reshape tabular data.

`krangl` is heavily inspired by the amazing [`dplyr`](https://github.com/hadley/dplyr) for [R](https://www.r-project.org/). `krangl` is written in [Kotlin](https://kotlinlang.org/), excels in Kotlin, but emphasizes as well on good java-interop. It is mimicking the API of `dplyr`, while carefully adding more typed constructs where possible.



[TOC levels=2,2]: # " "

- [Installation](#installation)
- [Features](#features)
- [Examples](#examples)
- [Documentation](#documentation)
- [How to contribute?](#how-to-contribute)


If you're not sure about how to proceed, check out  [krangl in 10 minutes](http://holgerbrandl.github.io/krangl/10_minutes/) section in the
**[krangl user guide](http://holgerbrandl.github.io/krangl/)**.


Installation
------------

To get started simply add it as a dependency to your `build.gradle`:

```groovy
repositories {
    mavenCentral() 
}

dependencies {
    implementation "com.github.holgerbrandl:krangl:0.16.1"
}
```
Declaring the repository is purely optional as it is the default already.

You can also use [JitPack with Maven or Gradle](https://jitpack.io/#holgerbrandl/krangl) to build the latest snapshot as a dependency in your project.

```groovy
repositories {
    maven { url 'https://jitpack.io' }
}
dependencies {
    implementation 'com.github.holgerbrandl:krangl:-SNAPSHOT'
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

* [Krangl User Guide](http://holgerbrandl.github.io/krangl) for detailed information about the API and usage examples.
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

* [KT-24789](https://youtrack.jetbrains.net/issue/KT-24789) "Unresolved reference" when running a script which is a symlink to a script outside of source roots
* [KT-12583](https://youtrack.jetbrains.com/issue/KT-12583) IDE REPL should run in project root directory
* [KT-11409](https://youtrack.jetbrains.com/issue/KT-11409) Allow to "Send Selection To Kotlin Console"
* [KT-13319](https://youtrack.jetbrains.net/issue/KT-13319) Support ":paste" for pasting multi-line expressions in REPL
* [KT-21224](https://youtrack.jetbrains.net/issue/KT-21224) REPL output is not aligned with input