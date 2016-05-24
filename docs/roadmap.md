Kplyr Roadmap
==============


Initial Release
-------------

* travisCI integration [done]
* first basic tests [done]
* rewrite table reader for better performance
* table write support using csv-commons wrapper [done]
* implement joins
* implement count and distinct [done]
* remove regrouping in core verbs where possible
* basic benchmarking framework (without jvm usage)
* consider to use invoke for row access



Future Plans
-------------

https://github.com/holgerbrandl/kplyr/issues

* golem bindings

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