Krangl Release History
======================

v0.4 (Not yet released)
----

* spread-gather support for data reshaping (fixes [#2](https://github.com/holgerbrandl/krangl/issues/2))
* improve reshaping functionality by adding `unite` and `separate` (fixes [#9](https://github.com/holgerbrandl/krangl/issues/9))
* `mutate()` can now change existing columns without altering column positions
* New property accessor  `DataFrame.cols` to access all columns of a data-frame


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

