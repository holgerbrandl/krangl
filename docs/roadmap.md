Kplyr Roadmap
==============


Intial Release
-------------

* travisCI integration


Future Plans
-------------


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

* Provide conversion to [adhoc](https://kotlinlang.org/docs/reference/object-declarations.html#object-expressions)/data class objects
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
