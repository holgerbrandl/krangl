Krangl Release History
======================

v0.9
----

In progress

* Added receiver context for sortBy lambdas with sorting specific API


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

[sdf](file:///Users/brandl/Desktop/README.pdf)
[sdf](file:///Users/brandl/Desktop/README__DOES_NOT_EXIST.pdf)
