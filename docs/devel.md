


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



## Useful libraries

* https://github.com/mplatvoet/progress
* https://github.com/SalomonBrys/Kodein cool dependency injection
* https://github.com/hotchemi/khronos date extension
