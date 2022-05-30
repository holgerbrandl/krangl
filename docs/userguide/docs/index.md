[ ![Download](https://img.shields.io/badge/Maven%20Central-0.18-orange) ](https://mvnrepository.com/artifact/com.github.holgerbrandl.krangl/krangl)  [![Build Status](https://github.com/holgerbrandl/krangl/workflows/build/badge.svg)](https://github.com/holgerbrandl/krangl/actions?query=workflow%3Abuild) [![Gitter](https://badges.gitter.im/holgerbrandl/krangl.svg)](https://gitter.im/holgerbrandl/krangl?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

This is the manual of [krangl](https://github.com/holgerbrandl/krangl).

`krangl` is an open-source {K}otlin library for data w{rangl}ing. By implementing a grammar of data manipulation using a modern functional-style API, it allows to filter, transform, aggregate and reshape tabular data.

`krangl` tries to become what pandas is for `python`, and `readr`+`tidyr`+`dplyr` are for R.

`krangl` is open-source and [developed](https://github.com/holgerbrandl/krangl) on github.

For a first primer see KotlinConf 2019 slides about [Data Science with kotlin](https://holgerbrandl.github.io/data_science_with_kotlin/data_science_with_kotlin.html#1)



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

Furthermore, it provides methods to go back and forth between untyped and typed data.


Installation
------------

To get started simply add it as a dependency:
```groovy
repositories {
    mavenCentral()
}

dependencies {
    implementation "com.github.holgerbrandl:krangl:0.18"
}
```
Declaring the repository is purely optional as it is the default already.

If you're very new to Kotlin and Gradle you may want to read first about its [basic syntax](https://kotlinlang.org/docs/reference/basic-syntax.html), some basic [IDE features](https://kotlinlang.org/docs/tutorials/getting-started.html) and about [how to use gradle](https://kotlinlang.org/docs/reference/using-gradle.html) to configure dependencies in Kotlin projects.

Example
-------

Flights that departed NYC, are grouped by date, some columns of interest are selected, dasummarized to reveal mean departure and arrival delays, and finally just those dates are kept that show extreme delays.

```kotlin
flights
    .groupBy("year", "month", "day")
    .select({ range("year", "day") }, { listOf("arr_delay", "dep_delay") })
    .summarize(
            "mean_arr_delay" to { it["arr_delay"].mean(removeNA = true) },
            "mean_dep_delay" to { it["dep_delay"].mean(removeNA = true) }
    )
    .filter { (it["mean_arr_delay"] gt  30)  OR  (it["mean_dep_delay"] gt  30) }
```