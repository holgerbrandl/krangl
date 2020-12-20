# `krangl` in 3 Minutes

Welcome to `krangl`. Relational data and how to handle it properly is a huge topic, but the core concepts are relatively simple. So let's get started!

## Columns and Rows


DataFrames are _just_ tables with type constraints within each column. To glance into them horizontally and vertically we can do

```kotlin
irisData.print(maxRows=10)
irisData.schema()
```

`irisData` is bundled with krangl, and  gives the measurements in centimeters of the variables sepal length and width and petal length and width, respectively, for 50 flowers from each of 3 species of iris. The species are Iris setosa, versicolor, and virginica.

![](https://upload.wikimedia.org/wikipedia/commons/thumb/4/49/Iris_germanica_%28Purple_bearded_Iris%29%2C_Wakehurst_Place%2C_UK_-_Diliff.jpg/800px-Iris_germanica_%28Purple_bearded_Iris%29%2C_Wakehurst_Place%2C_UK_-_Diliff.jpg)

Columns and Rows can be accessed using `krangl` with

```kotlin
val col = irisData["Species"]
val cell = irisData["Species"][1]
```

## Get your data into `krangl`

To save a data frame simply use

```kotlin
irisData.writeCSV(File("my_iris.txt"))
```

To load a data-frame simply **{done}**

```kotlin
irisData.writeCSV(File("my_iris.txt"))
```

It allows to Read from tsv, csv, json, jdbc, e.g.

```kotlin
val tornados = DataFrame.readCSV(pathAsStringFileOrUrl)
tornados.writeCSV(File("tornados.txt.gz"))
```

`krangl` will guess column types unless the user provides a column type model.


You can also simply define new data-frames in place

```kotlin
val users : DataFrame = dataFrameOf(
    "firstName", "lastName", "age", "hasSudo")(
    "max", "smith" , 53, false,
    "eva", "miller", 23, true,
    null , "meyer" , 23, null
)
```

{% hint style="info" %}
`krangl` also allows to convert any iterable into a data-frame via reflection. See the section about [Nested Data](nested_data.md) for details.
{% endhint %}

### Other input formats

`krangl` also allows to read in json array data. For a complete overview see [JsonIO](https://github.com/holgerbrandl/krangl/blob/master/src/main/kotlin/krangl/JsonIO.kt)

```kotlin
    val df = fromJson("my.json")
    val df2 = fromJson("http://foo.bar/my.json")
    
```


