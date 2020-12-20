# Data model of `krangl`


What is a DataFrame?

> A "tabular" data structure representing cases/records (rows), each of which consists of a number of observations or measurements (columns) [reference](https://github.com/mobileink/data.frame/wiki/What-is-a-Data-Frame%3F)

And by mapping this defintion to Kotlin code, we obtain the core abstraction of `krangl`:
```kotlin
interface DataFrame {
    val cols: List<DataCol>
}

abstract class DataCol(val name: String) {
    abstract fun values(): Array<*>
}
```
* Implemented as column model to allow for vectorization where possible
* Column implementations using nullable types `String?`, `Int?`, `Double?`, `Boolean?` and `Any?`
* Internal length and type consistency checks (e.g. prevent duplicated column names)



## To type or not to type?

* _Static types_ are cool, but most data has no type
* It's more robust/fun to use types and they allow for better design
* Many data attributes are very fluent

```kotlin
data class Employee(val id:Int, val name:String) 
val staffStats = listOf(Employee(1, "John"), Employee(2, "Anna"))  
    .predictNumSickDays()     // new type!
    .addPerformanceMetrics()  // new type!
    .addSalaries()            // new type!
    .correlationAnalysis()    // odd generic signature :-|
```
* R/python lack static typing, which make such workflows more fluent/fun to write

```r
staff %>% 
    mutate(sick_days=predictSickDays(name)) %>%   # table with another column
    left_join(userPerf) %>%                       # and some more columns
    left_join(salaries) %>%                       # and even more columns
    select_if(is.numeric) %>%                     
    correlate(type="spearman")                    # correlate numeric attributes
```


Defining types is a tedious process.

`krangl` allows to mix typed and untyped data in a tablular data structure:

`val dataFrame : DataFrame =`

| `employee:Employee` | `sales:List<Sale>` | `age:Int` | `address:String` | `salary:Double`   |
|:-----|:-------------|:----|:-----|:--|
| `Employee(23, "Max")` |    `listOf(Sale(...), Sale())` |   23  | "Frankfurt"     |  50.3E3 |
| ... |  ...    | ...    |  ...    | ...  |


It implements a  `pandas`/`tidyverse` like API to create, manipulate, reshape, combine and summarize  data frames

```kotlin
// aggregations like
dataFrame.groupBy("age").count()
dataFrame.summarize("mean_salary"){ mean(it["salaray"])}

// integration like
val df: DataFrame = dataFrame.leftJoin(otherDF)

// transformations like
dataFrame.addColumn("intial"){ it["employee"].map<Employee>{ it.name.first() }}
```



# Get your data into krangl


It allows to read from tsv, csv, json, jdbc, e.g.

```kotlin
val users = dataFrameOf(
    "firstName", "lastName", "age", "hasSudo")(
    null , "meyer" , 23, null)
 
val tornados = DataFrame.readCSV(pathAsStringFileOrUrl)
tornados.writeCSV(File("tornados.txt.gz"))
```

* Guess column types & default parameters
* Built-in missing value support

Convert any iterable into a data-frame via extension function + reflection

```kotlin
data class Person(val name:String, val address:String)
val persons : List<Person> = ...

val personsDF: DataFrame = persons.asDataFrame() 
```
