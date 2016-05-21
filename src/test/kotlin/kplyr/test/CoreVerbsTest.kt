package kplyr.test

import io.kotlintest.specs.FlatSpec
import kplyr.*


class CompoundTests : FlatSpec() { init {
    "it" should "summarize sleep data" {
        val df = DataFrame.fromCSV(DataFrame::class.java.getResourceAsStream("data/msleep.csv"))

        df.filter { it["awake"] gt 3 }.
                apply { glimpse() }.
                mutate("rem_proportion", { it["sleep_rem"] + it["sleep_rem"] }).
                groupBy("vore").
                summarize("mean_rem_prop", { it["rem_proportion"].mean() }).
                print()
    }
}
}

class SelectTest : FlatSpec() { init {

    "it" should "select with regex" {
        throw UnsupportedOperationException()
    }

    "it" should "select non-existing column" {
        throw UnsupportedOperationException()
    }

    "it" should "select no columns" {
        throw UnsupportedOperationException()
    }

    "it" should "select same columns twice" {
        throw UnsupportedOperationException()
    }
}
}

class FilterTest : FlatSpec() { init {
    "it" should "regrouping of data" {
        throw UnsupportedOperationException()
    }

    "it" should "filter in empty table" {
        throw UnsupportedOperationException()
    }
}
}

class Playground : FlatSpec() { init {
    "it" should "regrouping of data" {

    }
}
}