Krangl Roadmap
==============


Upcoming Releases
-----------------

* remove regrouping in core verbs where possible
* consider to use invoke for row access (potentially decouple more arguable extensions in different namespace?)
* provide eqivalent for dplyr::summarize_each and dplyr::mutate_each [#4](https://github.com/holgerbrandl/krangl/issues/4)


Performance optimization
------------------------

* [ ] 30% flights HOTSPOT: `krangl/Extensions.kt:275` can we get rid fo the array creation?
* [ ] `krangl.head` should use view instead of copy
* [ ] `krangl.SimpleDataFrame.addColumn` should avoid `toMutatbleList`
* [ ] `get rid of other `toMutableList` and use view instead
* [ ] More consistent use of List vs using arrays as column datastore (see [array vs list](http://stackoverflow.com/questions/716597/array-or-list-in-java-which-is-faster)). This would avoid array conversion which are omnipresent in the API at the moment.

References
* https://softwarecave.org/2014/03/19/views-in-java-collections-framework/

Future Plans
-------------


https://github.com/holgerbrandl/krangl/issues

* golem bindings
* Add a `DataFrame.transpose()` method
* Make use of kotlin.Number to simplify API

* Use JvmName to allow for more strongly typed (see  http://stackoverflow.com/questions/29268526/how-to-overcome-same-jvm-signature-error-when-implementing-a-java-interface)
```kotlin
@JvmName("mutateString")
fun DataFrame.mutate(name: String, formula: (DataFrame) -> List<String>): DataFrame {
    if(this is SimpleDataFrame){
        return addColumn(StringCol(name, formula(this)))
    }else
        throw UnsupportedOperationException()
}

```

* directly access values with it["foo"] and not just column object. For the latter DataFrame.cols can be used
    * Not a good idea because all extension function would then be defined for common lists like List<Int> etc. It's more important to keep the namespace clear

* Provide adhoc/data class conversion for column model [adhoc](https://kotlinlang.org/docs/reference/object-declarations.html#object-expressions)/data class objects
```kotlin
val dataFrame = object : DataFrame() {
    val x = Factor("sdf", "sdf", "sdfd")
    val y = DblCol(Double.MAX_VALUE, Double.MIN_VALUE)
    val z = y + y
}


val newTable = with(dataFrame) {
    object : DataFrame() {
        val src = dataFrame

        val newCol = z + z
        val base = dataFrame
    }
}

newTable.newCol
newTable.src.x
```

* improve benchmarking by avoid jmv warmup with -XX:CompileThreshold=1 [src](http://stackoverflow.com/questions/1481853/technique-or-utility-to-minimize-java-warm-up-time)

* Integrate idoms to do enrichment testing with fisher test from [commons-math](http://commons.apache.org/proper/commons-math/apidocs/org/apache/commons/math3/distribution/HypergeometricDistribution.html)

* misc consider to use kotlin.collections.ArrayAsCollection



Related libraries 

https://github.com/lwhite1/tablesaw

* allows for column indicies to speed up access 


http://dexvis.com or github https://github.com/PatMartin/Dex


fast column storage
https://github.com/lemire/JavaFastPFOR
http://fastutil.di.unimi.it/

http://nd4j.org/


## Design

https://stackoverflow.com/questions/45090808/intarray-vs-arrayint-in-kotlin

Array<*> can be null