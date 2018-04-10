# Krangl User Guide

TODO write this! :-)


## Joins

https://blog.jooq.org/2016/07/05/say-no-to-venn-diagrams-when-explaining-joins/


## Nested Data

For an intro see the corresponding R documentation
* https://www.rdocumentation.org/packages/tidyr/versions/0.8.0/topics/unnest
* https://www.rdocumentation.org/packages/tidyr/versions/0.8.0/topics/nest


# Examples, examples,  and more examples

1. Add a suffix to some column names
```kotlin
// first select column names to be altered
irisData.names.filter { it.startsWith("Sepal") }.map {
    // second, apply renaming
    oldName -> irisData.rename(oldName to ("My" + oldName)) 
}
```


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


# Resources

Other great resources are

* http://garrettgman.github.io/tidying/
