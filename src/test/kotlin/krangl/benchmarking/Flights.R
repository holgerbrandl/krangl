devtools::source_url("https://raw.githubusercontent.com/holgerbrandl/datautils/v1.25/R/core_commons.R")

loadpack(nycflights13)

loadpack(microbenchmark) # see http://adv-r.had.co.nz/Profiling.html


benchResults <- microbenchmark(
#    flights <-read_tsv("/Users/brandl/projects/kotlin/krangl/src/test/resources/krangl/data/nycflights.tsv.gz"),
flights %>%
    group_by(year, month, day) %>%
    select(year:day, arr_delay, dep_delay) %>%
    summarise(
    arr = mean(arr_delay, na.rm = TRUE),
    dep = mean(dep_delay, na.rm = TRUE)
    ) %>%
    filter(arr > 30 | dep > 30)
,
## num of runs for variance estmation
times=30
)

benchResults

Unit: milliseconds

min       lq     mean   median       uq      max neval
15.76406 16.14338 16.82494 16.52762 17.08487 24.17547    30

#loadpack(ggplot2)
#autoplot(benchResults)

# compound test
flights %>%
    group_by(year, month, day) %>%
    select(year:day, arr_delay, dep_delay) %>%
    summarise(
    arr = mean(arr_delay, na.rm = TRUE),
    dep = mean(dep_delay, na.rm = TRUE)
    ) %>%
    filter(arr > 30 | dep > 30)
