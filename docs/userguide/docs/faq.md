## How to rewrite common SQL bits with `krangl`?

1. `select this, that from there where that >5`

```kotlin
there.select("this", "that").filter{ it["that"] gt 5 }
```

## Why doesn't krangl provide vectorized comparison operators?

Some (`+`, `-`, `*`, `!`) can be overridden for collections, but others cannot (e.g. all arithmetic and boolean comparison ops)

No vectorization for `>`,  `&&` `==`, etc. in table forumlas → Use function calls or not so pretty `gt`, `AND`, `eq`, etc.


## Can we build data science workflows with Kotlin?

First, should we? Yes, because

* R & Python fail to be scalable & robust solutions for data science
* Java is known for great dependency tooling & scalability
* Java as a language is less well suited for data-science (cluttered, legacy bits)


In Febuary 2018 Kotlin v1.0 was released. Designed with DSLs in mind it comes alongs With great features language such Type Inference, Extension Functions, Data Classes, or Default Parameters, making it a perfect choice to do *data science on the JVM*.


## How does `krangl` compare to what R/dplyr or python/pandas?



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

And the same snippet written in `dplyr`:

```r
flights %>%
    group_by(year, month, day) %>%
    select(year:day, arr_delay, dep_delay) %>%
    summarise(
        mean_arr_delay = mean(arr_delay, na.rm = TRUE),
        mean_dep_delay = mean(dep_delay, na.rm = TRUE)
    ) %>%
    filter(mean_arr_delay > 30 | mean_dep_delay > 30)
```

The biggest different are the comparison operators, which Kotlin does not allow to [be overridden](https://kotlinlang.org/docs/reference/operator-overloading.html) in a vectorized way.

And the same in `pandas`. **{no clue, PR needed here!}**


## How to add columns totals to data-frame?

```kotlin
val foo = dataFrameOf(
    "Name", "Duration", "Color")(
    "Foo", 100, "Blue",
    "Goo", 200, "Red",
    "Bar", 300, "Yellow")

val columnTotals = foo.cols.map {
    it.name to when (it) {
        is IntCol -> it.sum()
        else -> null // ignored column types
    }
}.toMap().run {
    dataFrameOf(keys)(values)
}


bindRows(foo, columnTotals).print()
```



## Further Reading?

[`krangl` presentation at Kotlin-Night in Frankfurt (March 2018)](https://holgerbrandl.github.io/kotlin4ds_kotlin_night_frankfurt//emerging_kotlin_ds_ecosystem.html)

* [Krangl Introduction](http://holgerbrandl.github.io/krangl/bier_slides_june2016/krangl_intro.html) A presentation from June 2016 ([sources](./docs/bier_slides_june2016/krangl_intro.md))