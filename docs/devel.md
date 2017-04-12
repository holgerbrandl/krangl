


From spark release notes:
> Unifying DataFrames and Datasets in Scala/Java: Starting in Spark 2.0, DataFrame is just a type alias for Dataset of Row. Both the typed methods (e.g. map, filter, groupByKey) and the untyped methods (e.g. select, groupBy) are available on the Dataset class. Also, this new combined Dataset interface is the abstraction used for Structured Streaming. Since compile-time type-safety in Python and R is not a language feature, the concept of Dataset does not apply to these languages’ APIs. Instead, DataFrame remains the primary programing abstraction, which is analogous to the single-node data frame notion in these languages. Get a peek from a Dataset API notebook.


# Interactive shell
```bash
kscript -i - <<"EOF"
//DEPS de.mpicbg.scicomp:krangl:0.4-SNAPSHOT
EOF
```

kotlinc  -classpath '/Users/brandl/.m2/repository/de/mpicbg/scicomp/krangl/0.3/krangl-0.3.jar:/Users/brandl/.m2/repository/org/apache/commons/commons-csv/1.3/commons-csv-1.3.jar:/Users/brandl/.m2/repository/org/jetbrains/kotlin/kotlin-stdlib/1.0.2/kotlin-stdlib-1.0.2.jar:/Users/brandl/.m2/repository/org/jetbrains/kotlin/kotlin-runtime/1.0.2/kotlin-runtime-1.0.2.jar'


<!-- sdk use kotlin 1.0.6 -->
kotlinc  -classpath '/Users/brandl/.m2/repository/de/mpicbg/scicomp/krangl/0.4-SNAPSHOT/krangl-0.4-SNAPSHOT.jar:/Users/brandl/.m2/repository/org/apache/commons/commons-csv/1.3/commons-csv-1.3.jar:/Users/brandl/.m2/repository/org/jetbrains/kotlin/kotlin-stdlib/1.1.0/kotlin-stdlib-1.1.0.jar:/Users/brandl/.m2/repository/org/jetbrains/annotations/13.0/annotations-13.0.jar'



# How to release a new version



1) Increment version in build.gradle
2) Update version in README.md and CHANGS.md
2) Push and and create version on github
3) Build assembly jar, build tar.gz with:

```
gradle install

cd ~/.m2/repository/de/mpicbg/scicomp/

## locate jar, sources.jar and pom for new version in
open ~/.m2/repository/de/mpicbg/scicomp/krangl

# Upload path
de/mpicbg/scicomp/krangl/0.X
```

(todo use https://github.com/softprops/bintray-sbt)


5) Create new version on [jcenter](https://bintray.com/holgerbrandl/mpicbg-scicomp/krangl/view`

6) Post-release

* Increment version to 1.x-SNAPSHOT in build.gradle
