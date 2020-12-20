{% hint style="info" %}
For a primer on tidy data read http://garrettgman.github.io/tidying/
{% endhint %}


# Example: Data Reshaping with `krangl`

```kotlin
val climate = dataFrameOf(
        "city", "coast_distance", "1995", "2000", "2005")(
        "Dresden", 400, 343, 252, 423,
        "Frankfurt", 534, 534, 435, 913)
```

```
     city   coast_distance   1995   2000   2005
  Dresden              400    343    252    423
Frankfurt              534    534    435    913
```

```kotlin
climate. gather("year", "rainfall", columns = { matches("[0-9]*")} )
```

```
     city   coast_distance   year   rainfall
  Dresden              400   1995        343
Frankfurt              534   1995        534
  Dresden              400   2000        252
Frankfurt              534   2000        435
  Dresden              400   2005        423
Frankfurt              534   2005        913
```

???

`colummns` use function literals again, with column names type as receiver

---
# Example: Data Ingestion with `krangl`

```kotlin
dataFrameOf("user")("brandl,holger,37")
        .apply { print() }
        .separate("user", listOf("last_name", "first_name","age"), convert = true)
        .apply { print() }
        .apply { glimpse() }
```

```
            user
brandl,holger,37
```
-----
```
last_name   first_name   age
   brandl       holger    37
```
-----
```
DataFrame with 1 observations
last_name  : [Str]	, [brandl]
first_name : [Str]	, [holger]
age        : [Int]	, [37]
```

# Digest objects into attribute columns

Cherry-pick properties with `Iterable<T>.deparseRecords`
```kotlin
val deparsedDF = records.deparseRecords { mapOf(
    "age" to it.age, 
    "weight" to it.mean_weight
) }

```

Be lazy and use reflection
```kotlin
data class Person(val name:String, val age:Int)
val persons :List<Person> = listOf(Person("Max", 23), Person("Anna", 43))

val personsDF: DataFrame = persons.asDataFrame() 
personsDF
```

```
age   name
 23   Max
 43   Anna
```

# List/object columns

`krangl` supports arbitrary types per column

```kotlin
val persons: DataFrame = dataFrameOf("person")(persons) 
persons
```

```
                      person
   Person(name=Max, age=23)
   Person(name=Anna, age=43)
```

```kotlin
personsDF2.glimpse()
```

```
DataFrame with 2 observations
person	: [Any]	, [Person(name=Max, age=23), Person(name=Anna, age=43)]
```



# Unfold objects into columns

* similar to `separate()` but for object columns


```kotlin
data class Person(val name:String, val age:Int)
val persons :Iterable<Person> = listOf(Person("Max", 22), Person("Anna", 23))

val df : DataFrame = dataFrameOf("person")(persons)

df.names
```
```
["person"]
```
--

Expand properties of `person` into columns via reflection

```kotlin
var personsDF = df.
    unfold<Person>("person", keep=true) 
    // unfold<Person>("person", select=listOf("age"))
    
personsDF.names   
```

```
["person", "name", "age"]
```


# Let krangl define the schema


Infer a schema with

```kotlin
irisData.printDataClassSchema("Iris")
```
which makes krangl to __print__ the Kotlin data class schema for data frame:

```kotlin
data class Iris(val sepalLength: Double, val sepalWidth: Double, val petalLength: Double, 
                val petalWidth: Double, val species: String)
                
val records: Iterable<Iris> = irisData.rowsAs<Iris>()
```

Paste it back into workflow code and continue with typed objects!

```kotlin
records.take(1)
```

```
[ Iris(sepalLength=5.1, sepalWidth=3.5, petalLength=1.4, petalWidth=0.2, species=setosa) ]
```
