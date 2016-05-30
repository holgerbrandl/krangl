package kplyr.benchmarking

import kplyr.*
import kplyr.devel.RunTimes
import kotlin.system.measureTimeMillis

/**

Get data with
Rscript -e '
library(nycflights13)
require(readr)
write_tsv(flights, "nycflights.tsv")
'

cat nycflights.tsv | gzip -c > nycflights.tsv.gz

interactive kotlin repl
kotlinc -J"-Xmx8g" -cp /Users/brandl/projects/kotlin/kplyr/build/classes/main:/Users/brandl/.gradle/caches/modules-2/files-2.1/org.apache.commons/commons-csv/1.3/aeed8320e1b86b27e0b477a898eb7dd049526963/commons-csv-1.3.jar


echo '
# http://stackoverflow.com/questions/6262203/measuring-function-execution-time-in-r
require(readr)
flights <- read_tsv("/Users/brandl/projects/kotlin/kplyr/src/test/resources/kplyr/data/nycflights.tsv.gz")
system.time(read_tsv("/Users/brandl/projects/kotlin/kplyr/src/test/resources/kplyr/data/nycflights.tsv.gz"))
' | R --vanilla
```

Compressed data benchmarking
Original Impl:
data loading time was: 3.02 ± 1.11 SD

dplyr
user  system elapsed
0.553   0.022   0.575
 */


fun main2(args: Array<String>) {
    val flights = RunTimes.measure({
        DataFrame.fromTSV("/Users/brandl/Desktop/nycflights.tsv")
    }, 3).apply {
        println("data loading time was: $runtimes")
    }.result

    println ("done reading starting glimpsing")

    flights.names

    flights.glimpse()




    var groupedFlights: DataFrame = SimpleDataFrame()

    println("group flights took " + measureTimeMillis {
        groupedFlights = flights.groupBy("carrier")
    })

    println("summarizing carriers took " + measureTimeMillis {
        groupedFlights.summarize(
                "mean_arr_delay" to { it["arr_delay"].mean() }
        ).print()
    })
}


fun main3(args: Array<String>) {
    val fromCSV = RunTimes.measure({ DataFrame.fromCSV("/Users/brandl/projects/kotlin/kplyr/src/test/resources/kplyr/data/msleep.csv") }, numRuns = 10).apply {
        println(runtimes)
    }.result


//    fromCSV.print()
    fromCSV.glimpse()
}