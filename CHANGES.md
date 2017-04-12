Krangl Release History
======================

v0.4 (Not yet released)
----

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

