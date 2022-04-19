### Run using Gradle

* This will execute all (methods annotated with `@Benchmark`) benchmarks with their predefined parameters:

`krangl$ ./gradlew --console=plain clean jmh`

* Output is saved as CSV in `benchmarks/build/results/jmh/results.csv`

### Run directly

A bit clunky but gives more control over parameters and what is actually getting executed 

* Display command line options:
```
krangl/benchmarks$  java -jar build/libs/benchmarks-jmh.jar -h`
```

* Run specific benchmark(s) with specific parameters: 
```
krangl/benchmarks$ java -jar build/libs/benchmarks-jmh.jar -wi 2 -i 2 -f 1 -tu ms -bm avgt CompressedTsvBenchmarks
```