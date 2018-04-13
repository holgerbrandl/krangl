# cd /Users/brandl/projects/kotlin/krangl/examples/benchmarking; R
devtools::source_url("https://raw.githubusercontent.com/holgerbrandl/datautils/v1.49/R/core_commons.R")

load_pack(jsonlite)



# perfData = "perf_logs/benchmarks.csv" %>%
perfData = list.files("perf_logs", "jmh_results_*", full = T) %>%
    tail(1) %>%
    read_csv() %>%
    pretty_columns() %>%
    rename(ci = `score_error_99_9%`)

perfData %>%
    filter(mode == "avgt") %>%
    filter(benchmark == "org.krangl.performance.BackendBench.columnArithmetics") %>%
    ggplot(aes(as.factor(param_krows), score)) +
    geom_col() +
    geom_errorbar(aes(ymin = score - ci, ymax = score + ci), width = .2) +
    ggtitle("column arithmetics") #+ scale_x_log10()
    # coord_cartesian(ylim=c(0, 5))


## by using json we would have better access to raw data
# jsonData = fromJSON("perf_logs/benchmarks.csv")
# results = flatten(jsonData) %>% transmute(benchmark, runtime = primaryMetric.score, ci = primaryMetric.scoreError)
# results$benchmark %<>% str_match("Benchmark.(.*)") %>% get_col(2)
#
# # note 1% CI is used here
# results %>% ggplot(aes(benchmark, runtime)) +
#     geom_bar(stat = "identity") +
#     geom_errorbar(aes(ymin = runtime - ci, ymax = runtime + ci), width = .2)