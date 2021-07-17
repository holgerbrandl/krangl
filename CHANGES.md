krangl Release History
======================

v0.17 
-----

* New Jupyter kernel integration. See [sleep_patterns.ipynb](https://github.com/holgerbrandl/krangl/blob/master/examples/jupyter/sleep_patterns.ipynb) for an example/tutorial


v0.16 
-----

Released 2021-04-13

`krangl` is now deployed to maven-central and no no longer ot jcenter

Features
* Added support for fixed-width files with `readFixedWidth()`
* Added supported for more compact column type specification when reading tsv
* Fixed: NA and emtpy cell handling in excel-reader
* Fixed: Use correct cell types when writing Excel file

v0.15.6
-----

Republished to maven central <https://search.maven.org/artifact/com.github.holgerbrandl.krangl/krangl>

v0.15.2
-----

* Fixed `gather` conversion in case of mixed number types
* Indicate guessed column type with prefix Any for basic types in `schema` and `print`

v0.15.1
-----

* Fixed asDataFrame to include parent type properties
* Added `DataFrame.filterNotNull` to remove records will nulls. A column selector can be provided to check only a subset of columns.

v0.15
-----

New Features
* [#97](https://github.com/holgerbrandl/krangl/pull/97/) Added Excel read/write support (by [LeandroC89](https://github.com/LeandroC89))
```kotlin

// read
df = DataFrame.readExcel("data.xlsx", sheetName = "sales")
df = DataFrame.readExcel("data.xlsx", cellRange = CellRangeAddress.valueOf("A1:D10"))

// write
df.writeExcel("results.xslx")

```

* [#95](https://github.com/holgerbrandl/kscript/issues/95) Improved column type casts
```
dataFrameOf("foo")(1, 2, 3).addColumn("stringified_foo") { it["foo"].toStrings() }.schema()
> DataFrame with 3 observations
> foo              [Int]  1, 2, 3
> stringified_foo  [Str]  1, 2, 3

dataFrameOf("foo")("1", "2", "3").addColumn("parsed_foo") { it["foo"].toInts() }.schema()

> DataFrame with 3 observations
> foo         [Str]  1, 2, 3
> parsed_foo  [Int]  1, 2, 3
```

* [#99](https://github.com/holgerbrandl/kscript/issues/99) Added filtering by list (similar to R's `%in%` operator)
```kotlin
irisData.filter { it["Species"].inList("setosa", "versicolor")  }
```

Bug Fixes
* [#84](https://github.com/holgerbrandl/kscript/issues/84) Builder now supports mixed numbers in column
* [#96](https://github.com/holgerbrandl/kscript/issues/96) & [#94](https://github.com/holgerbrandl/kscript/issues/94) Fixed bugs in `join`
* [#100](https://github.com/holgerbrandl/kscript/issues/100) Improved SQL bindings
* [#99](https://github.com/holgerbrandl/kscript/issues/99) Fixed median

v0.14
-----

* Fixed missing by values overhanging RHS in outer join (fixes [#94](https://github.com/holgerbrandl/krangl/issues/94))
* Added addRow (via [PR92](https://github.com/holgerbrandl/krangl/pull/92) by [LeandroC89](https://github.com/LeandroC89)
* Added column type text to sql interface (fixes [#72](https://github.com/holgerbrandl/krangl/issues/72))

v0.13
-----

Released: 2020-06-02

* Added column transformation to calculate cumulative sum `cumSum`
```
sales
    .sortedBy("quarter")
    .addColumn("cum_sales" to { it["sold_units"].cumSum()})
```

* Added  column transformation `pctChange` to calculate percentage change between the current and a prior element. similar to [pct_change](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.pct_change.html) in pandas (contributed by @amorphous1 in [PR85](https://github.com/holgerbrandl/krangl/pull/85))
```kotlin
sales
    .groupBy("product")
    .addColumn("sales_pct_change" to { it["sold_units"].pctChange() })
```

* Added `lead` and `lag` (contributed by @amorphous1 in [PR85](https://github.com/holgerbrandl/krangl/pull/88))
```kotlin
sales
    .groupBy("product")
    .sortedBy("quarter")
    .addColumn("prev_quarter_sales" to { it["sold_units"].lag() })
```

* Significantly improved join performance (contributed by @amorphous1 in [PR85](https://github.com/holgerbrandl/krangl/pull/85))


* New: Extended `bindRows` API to combine data rowwise (see PR [#77](https://github.com/holgerbrandl/krangl/issues/77)
 by @CrystalLord)
```kotlin
val person1 = mapOf("person" to "James", "year" to 1996)
val person2 = mapOf("person" to "Anne", "year" to 1998)

emptyDataFrame().bindRows(person1, person2).print()
```


v0.12
-----

internal release

v0.11
----


* New: Added built-it support for `Long` columns (PR [#69](https://github.com/holgerbrandl/krangl/issues/69)
 by @davidpedrosa)


v0.10
-----

Major:
* New: `summarizeAt` for simplified column aggregations
* New: `setNames` to replace column headers of a data-frame
* New: Deparse Iterables more conveniently using lambdas in `deparseRecords`

Minor:
* Fixed: Can not read csv-tables without header
* Added option to skip lines in csv reader.
* Fixed `schema()` should no throw memory exception ([#53](https://github.com/holgerbrandl/krangl/issues/53): )
* Fixed `DataFrame.readTSV` default format ([#56](https://github.com/holgerbrandl/krangl/issues/56))
* Added `where()` for conditional column creation (relates to [#54](https://github.com/holgerbrandl/krangl/issues/54))
* Added `writeTSV`
* Fixed grouping by `Any` columns
* Added: `toDoubleMatrix()` helper extension method

v0.9.1
-----

Major Enhancements

* `DataFrame.fromJson` will now flatten nested json data

Minor

* Added `sum()` extension for columns summaries/transformation
* Added `dataFrameOf()` that accepts Iterable of names
* Added `bindRows()` alias that accepts data frames as varargs
* Added `bindCols()` extension for list of `DataCol`
* Fill missing cells with NA in `bindRows` and `bindCols`
* Resolve duplicated column names in `bindCols()`
* Added new builder to create data-frame from `DataFrameRow` iterator
* Added `addRowNumber` to add the row number as column to a data-frame
* Fixed: Incorrect types in gathered columns


v0.9
----

Released 2018-04-11

Major Enhancements

* Allow index access for column model (fixes [#46](https://github.com/holgerbrandl/krangl/issues/46)): `irisData[1][2]`
* Improved `DataFrame.count` to respect existing groupings and to simply count rows if no grouping is defined
* Added `moveLeft` and `moveRight` to rearrange column order
* Added `nest` and `unnest` to wrap columns into sub-tables and back
* Added `expand` and `complete` to expand column value-sets into data-frames
* Added function literal support for `count` and `groupBy` (fixes [#48](https://github.com/holgerbrandl/krangl/issues/48)): `irisData.groupByExpr{ it["Sepal.Width"] > 3 }`
* Added receiver context for sortBy lambdas with sorting specific API (fixes [#44](https://github.com/holgerbrandl/krangl/issues/44))

Improved data-frame rendering

* Improved `print()`ing of data-frames and `schema()`ta to have better alignment and more formatting options
* Print row numbers by default when using `print` (fixes [#49](https://github.com/holgerbrandl/krangl/issues/49))


Minor Enhancements

* Renamed `select2`/`remove2` to `selectIf` and `removeIF`
* Fixed #39: Can not add scalar object as column
* Started submodule for documentation
* Hide columns in `print` after exceeding maximum line length (fixes [#50](https://github.com/holgerbrandl/krangl/issues/50))
* Fixed [#45](https://github.com/holgerbrandl/krangl/issues/45): `sleepData.sortedBy{ "order" }` should fail with informative exception


v0.8
----

Released 2018-03-21

Major Enhancements

* Added property unfolding `df.unfold<Person("user", properties=listOf("address"))`
* Added text matching helper: `irisData.filter{ it["Species"].isMatching{ startsWith("se") }}` (fixes #21)
* Added `sortedByDescending` and `desc` and added more sorting tests
* Added More elegant object bindings via reflection. Example `val objPersons : Iterable<User> = users.rowsAs<User>()` (fixes #22)
* Added compressed csv write support, configurable or by filename guessing

Minor Enhancements

* More robust row to object conversion
* Made `List<Boolean?>.not()` public
* Use regex instead of string as `separate` separator
* Replaced fixed temporary column names with uuids
* Fixed incorrect coercion of incomplete inplace data to df
* Added `concat` operator for string column arithmetics
* Fixed arithmetic comparison operators
* Added beakerx display adapter


v0.7
----

Released 2018-03-14

Major Enhancements
* Allow specifying column types when reading csv data (Thanks to [LeanderG](https://github.com/LeanderG) for providing the [PR](https://github.com/holgerbrandl/krangl/pull/28))
* Added `groupedBy` to provide distinct set of grouping tuples as data-frame
* Read support for URLs (Example `DataFrame.readCSV("https://git.io/vxks7").glimpse()`)
* Added basic read/write support for JSON data
* Added generic collection conversion `Iterable<Any>.asDataFrame()` via reflection (fixes [#24](https://github.com/holgerbrandl/krangl/issues/24))


Incompatible API changes
* Renamed `structure` to `columnTypes`
* Renamed all table read function from `.from*` to `.read*`
* Fixed [#29](https://github.com/holgerbrandl/krangl/issues/29): `mapNonNull` should use parameter and not receiver


Minor Enhancements
* Namespace cleanup to hide internal helpers
* Bundled `irisData`
* Enhanced: `DataCol.toDouble()` should work for int columns as well (same vv)
* Added MIT License
* Use iterable instead of list for object conversions


v0.6
----
Released: 2017-11-11

* More idiomatic API mimicking kotlin stdlib where possible
* Added `DataFrame.remove` to drop columns from data-frames
* Added `DataFrame.addColumn` to add column from data-frames
* Added `DataFrame.sortBy(TableFormula)`
* Added `DataFrame.filterByRow`
* Reworked column selector API
* Changed column expression API from Any to a constrained set of support types
* Fixed issues when combining columns of different types (e.g. DoubleCol + IntCol
* Dropped most unary operators

v0.5
----

Skipped.

v0.4
----

released on 2017-4-12

New Features

* `spread()`-`gather()` support for elegant data reshaping (fixes [#2](https://github.com/holgerbrandl/krangl/issues/2))
* Improve reshaping functionality by adding `unite` and `separate` (fixes [#9](https://github.com/holgerbrandl/krangl/issues/9))
* Added `sampleFrac()` and `sampleN()` for random sub-sampling of data-frames (either with or without replacement)

Important Bug Fixes
* `mutate()` can now change existing columns without altering column positions

Other
* New property accessor  `DataFrame.cols` to access all columns of a data-frame
* Incremented kotlin version to 1.1


v0.3
----

Initial Release

* Implement all `dplyr` core verbs
* Implement all join types
* Table write support using csv-commons wrapper
* Extensive unit test coverage =
* TravisCI integration
* Support for `count()` and `distinct()`
* Basic benchmarking framework (without jvm usage)
