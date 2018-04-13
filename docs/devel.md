# Developer Notes for Krangl

[TOC]: # " "

- [Developer Notes for Krangl](#developer-notes-for-krangl)
- [Interactive shell](#interactive-shell)
- [Potentially useful libraries](#potentially-useful-libraries)
- [Design](#design)
    - [Receiver vs parameter functions vs properties](#receiver-vs-parameter-functions-vs-properties)
    - [gradle](#gradle)
- [Comparison to other APIs](#comparison-to-other-apis)
    - [Known differences to `dplyr` package in R](#known-differences-to-dplyr-package-in-r)
    - [Spark](#spark)
    - [tablesaw](#tablesaw)




# Interactive shell

```bash
kscript -i - <<"EOF"
//DEPS de.mpicbg.scicomp:krangl:0.9-SNAPSHOT
EOF
```

# Potentially useful libraries

* https://github.com/mplatvoet/progress
* https://github.com/SalomonBrys/Kodein cool dependency injection
* https://github.com/hotchemi/khronos date extension
* https://github.com/zeroturnaround/zt-exec cool process builder api


# Design

https://stackoverflow.com/questions/45090808/intarray-vs-arrayint-in-kotlin --> bottom line: Array<*> can be null

## Receiver vs parameter functions vs properties

How to write vector utilties?

```
dataFrame.summarize("mean_salary") { mean(it["salaray"]) }    // function parameter 
dataFrame.summarize("mean_salary") { it["salaray"].mean() }   // extension/member function
dataFrame.summarize("mean_salary") { it["salaray"].mean }     // extension property
```

???

Don't overload `operator Any?.plus` --> Confusion

https://kotlinlang.org/docs/reference/operator-overloading.html



## gradle

create fresh gradle wrapper with:

`gradle wrapper --gradle-version 4.2.1`

From https://github.com/twosigma/beakerx/issues/5135: Split repos?
> It is a bad idea. Many different repos are hard to maintain. And you do not need this. Gradle allows to publish separate artifacts without splitting repository.  
you can use `gradle :kernel:base:<whatever>` instead of `cd`.

---

http://stackoverflow.com/questions/29268526/how-to-overcome-same-jvm-signature-error-when-implementing-a-java-interface

To Improve JVM compatibility use JvmName to allow for more strongly typed

```kotlin
@JvmName("mutateString")
fun DataFrame.mutate(name: String, formula: (DataFrame) -> List<String>): DataFrame {
    if(this is SimpleDataFrame){
        return addColumn(StringCol(name, formula(this)))
    }else
        throw UnsupportedOperationException()
}

```





# Comparison to other APIs


And the same in `pandas`. **{PR needed here}**


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


## Spark


From spark release notes:
> Unifying DataFrames and Datasets in Scala/Java: Starting in Spark 2.0, DataFrame is just a type alias for Dataset of Row. Both the typed methods (e.g. map, filter, groupByKey) and the untyped methods (e.g. select, groupBy) are available on the Dataset class. Also, this new combined Dataset interface is the abstraction used for Structured Streaming. Since compile-time type-safety in Python and R is not a language feature, the concept of Dataset does not apply to these languages’ APIs. Instead, DataFrame remains the primary programing abstraction, which is analogous to the single-node data frame notion in these languages. Get a peek from a Dataset API notebook.

## tablesaw

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


