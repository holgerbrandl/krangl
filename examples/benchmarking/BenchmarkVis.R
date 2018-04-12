# Title     : TODO
# Objective : TODO
# Created by: brandl
# Created on: 9/12/17

devtools::source_url("https://raw.githubusercontent.com/holgerbrandl/datautils/v1.42/R/core_commons.R")
load_pack(dplyr)
load_pack(ggplot2)
load_pack(magrittr)
load_pack(stringr)

load_pack(jsonlite)

jsonData = fromJSON("/Users/brandl/projects/spark/component_labeling/results.csv")
flatten(jsonData) %>% select(- contains("scorePercentiles"), - primaryMetric.rawData)


results = flatten(jsonData) %>% transmute(benchmark, runtime = primaryMetric.score, ci = primaryMetric.scoreError)
results$benchmark %<>% str_match("Benchmark.(.*)") %>% get_col(2)

# note 1% CI is used here
results %>% ggplot(aes(benchmark, runtime)) +
    geom_bar(stat = "identity") +
    geom_errorbar(aes(ymin = runtime - ci, ymax = runtime + ci), width = .2)