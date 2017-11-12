# Krangl User Guide

TODO write this! :-)

learn from https://jtablesaw.wordpress.com/an-introduction/


## Introduction


krangl vs. dplyr
--------------

`krangl` is inspired by the excellent R-package [`dplyr`](http://dplyr.tidyverse.org/). Here's an example using airline on-time data for all [flights departing NYC in 2013](https://cran.r-project.org/web/packages/nycflights13/index.html).

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
    .select({ range("year", "day") }, { listOf("arr_delay", "dep_delay") })
    .summarize(
            "mean_arr_delay" to { it["arr_delay"].mean(removeNA = true) },
            "mean_dep_delay" to { it["dep_delay"].mean(removeNA = true) }
    )
    .filter { (it["mean_arr_delay"] gt  30)  OR  (it["mean_dep_delay"] gt  30) }
```
The biggest different are the comparison operators, which Kotlin does not allow to [be overridden](https://kotlinlang.org/docs/reference/operator-overloading.html) in a vectorized way.

For sure `dplyr` goes way beyond over what is possible with `krangl` at the moment (e.g. database access, 10x better performance). Also other R packages crucial for data science are not yet available in Kotlin. We aim to provide at least few of them as detailed out in our roadmap.


## Core Verbs

### Create New Columns

To make create columns starting with constant values those need to be expanded to static columns using with `const`
```
df.createColumn("user_id") { const("id") + nrow }

```


## Joins


https://blog.jooq.org/2016/07/05/say-no-to-venn-diagrams-when-explaining-joins/

## Known differences to `dplyr` package in R

* `rename()` will preserve column positions whereas `dplyr::rename` add renamed columns to the end of the table
* The mapping order is inverted in `rename()`. Instead of
   ```
   dplyr::rename(data, new_name=old_name)
   ```
   the krangl syntax is inverted to be more readible
   ```
   data.rename("old_name" to "new_name")
   ```
* `sortedBy()` will sort by grouping attributes first, and then per group with the provided sorting attributes.
* `select()` does not silently ignore multiple selections of the same column, but throws an error instead
* `select()` will throw an error if a grouping column is being removed (see [dplyr ticket](https://github.com/hadley/dplyr/issues/1869))


## Comparison to other APIs


References & Related Projects
-----------------------------

Most relavant
* https://github.com/jtablesaw/tablesaw which is the supposedly The simplest way to slice data in Java

| Feature                | Krangl | TableSaw |
|:-----------------------|:-------|:---------|
| Kotlin API             | Yes    | Yes      |
| Add column             | df.    |          |
| Select columns by type |        |          |



Select columns by type
* krangl
```
df.select( 
```

* tablesaw
```
val df = Dataframe(df.structure().target.selectWhere(column("Column Type").isEqualTo("INTEGER")))
```

