package kplyr.benchmarking

import kplyr.*

/**

Get data with
Rscript -e '
library(nycflights13)
require(readr)
write_tsv(flights, "nycflights.tsv")
'

cat nycflights.tsv | gzip -c > nycflights.tsv.gz
 */


fun main(args: Array<String>) {
    println ("starting reading")

    val flights = fromTSV("/Users/brandl/Desktop/nycflights.tsv")
    println ("done reading starting glimpsing")

    flights.glimpse()

    println("group flights")
    val groupedFlights = flights.groupBy("carrier")

    println("summarizing carriers")

    groupedFlights.summarize(
            "mean_arr_delay" to { it["arr_delay"].mean() }
    ).print()
}