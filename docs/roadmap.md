Krangl Roadmap
==============

For completed items see [change-log](../CHANGES.md).

## Next release

https://github.com/holgerbrandl/krangl/issues

* [provide a `tidyr::fill` equivalent](https://github.com/holgerbrandl/krangl/issues/61)
* [consider shorter columns cast as in pandas](https://github.com/holgerbrandl/krangl/issues/60)
* [Add excel import/export](https://github.com/holgerbrandl/krangl/issues/58)
* Better documentation & cheatsheet
* Date (column?) support
* Factor (column?) support + [Add factor attribute utilities similar to methods in R package `forcats`](https://github.com/holgerbrandl/krangl/issues/47)

* better spec out NA
    * consider use of doublearray for double/int-col along with NaN, see https://pandas.pydata.org/pandas-docs/stable/missing_data.html#working-with-missing-data


## Meta

* inconsistenly named reader methods
* `krangl.ColumnsKt#map` should have better return type
* use/support compressed columns (https://github.com/lemire/JavaFastPFOR)
* Better lambda receiver contexts
* Performance (indices, avoid list and array copies, compressed columns)
* Use dedicated return type for table formula helpers (like `mean`, `rank`) to reduce runtime errors
* More bindings to other jvm data-science libraries
* `Sequence` vs `Iterable`?
* Pluggable backends like native or SQL
* should `unfold` be better called `flatten`?

* write chapter about timeseries support
    * https://github.com/signaflo/java-timeseries/wiki/ARIMA-models
    * learn from https://pandas.pydata.org/pandas-docs/stable/timeseries.html


### IO

* Add parquet support https://stackoverflow.com/questions/39728854/create-parquet-files-in-java

### Core

* more defined behavior/tests needed for grouped dfs that become empty after filtering
```r
require(dplyr)
iris %>% group_by(Species) %>% filter(Sepal.Length>100)

```


---
### API improvements


---
### Performance

* misc consider to use kotlin.collections.ArrayAsCollection
* Setup up benchmarking suite

List copy optimization
* use iterable where possible
* misc consider to use kotlin.collections.ArrayAsCollection --> get rid of toList which always does a full copy internally.
* [ ] 30% flights HOTSPOT: `krangl/Extensions.kt:275` can we get rid fo the array creation?
* [ ] `krangl.SimpleDataFrame.addColumn` should avoid `toMutatbleList`
* [ ] More consistent use of List vs using arrays as column datastore (see [array vs list](http://stackoverflow.com/questions/716597/array-or-list-in-java-which-is-faster)). This would avoid array conversion which are omnipresent in the API at the moment.
* [ ] `get rid of other `toMutableList` and use view instead
* Analyze benchmark results with with kravis/krangl :-)


* use for column indices to speed up access

fast column storage
https://github.com/lemire/JavaFastPFOR
http://fastutil.di.unimi.it/

http://nd4j.org/

benchmarking

https://github.com/mm-mansour/Fast-Pandas

Backlog
-------

* remove regrouping in core verbs where possible
* consider to use invoke for row access (potentially decouple more arguable extensions in different namespace?)
* provide equivalent for dplyr::summarize_each and dplyr::mutate_each [#4](https://github.com/holgerbrandl/krangl/issues/4)

* `krangl.head` should use view instead of copy; also consider to use views for grouped data (see https://softwarecave.org/2014/03/19/views-in-java-collections-framework/)


* koma bindings --> http://koma.kyonifer.com/
* Add a `DataFrame.transpose()` method `as_tibble(cbind(nms = names(df), t(df)))`


* Integrate idoms to do enrichment testing with fisher test from [commons-math](http://commons.apache.org/proper/commons-math/apidocs/org/apache/commons/math3/distribution/HypergeometricDistribution.html)


* see tablesaw changelog https://jtablesaw.github.io/tablesaw/changes_in_v_0.2


## Rejected Ideas


* directly access values with `it["foo"]` and not just column object. For the latter DataFrame.cols can be used
    * Not a good idea because all extension function would then be defined for common lists like List<Int> etc. It's more important to keep the namespace clear


---

Provide adhoc/data class conversion for column model [adhoc](https://kotlinlang.org/docs/reference/object-declarations.html#object-expressions)/data class objects
```kotlin
val dataFrame = object : DataFrame() {
    val x = Factor("sdf", "sdf", "sdfd")
    val y = DblCol(Double.MAX_VALUE, Double.MIN_VALUE)
    val z = y + y
}


val newTable = df.map{ data class Foo(val name:String)}

newTable.newCol
newTable.src.x
```

-->  Can not work because data class is not an expression

---

* improve benchmarking by avoid jmv warmup with -XX:CompileThreshold=1 [src](http://stackoverflow.com/questions/1481853/technique-or-utility-to-minimize-java-warm-up-time)

--> rather continue with jmh driven benchmarking subproject


--

Make use of kotlin.Number to simplify API --> Done by adding `NumberCol` but unclear how to actually benefit from it
