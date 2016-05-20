package kplyr.test

import io.kotlintest.specs.FlatSpec
import kplyr.DataFrame
import kplyr.fromCSV
import kplyr.glimpse


class CompoundTests : FlatSpec() { init {
    "it" should "select with regex" {
        val df = DataFrame.fromCSV(DataFrame::class.java.getResourceAsStream("/data/msleep.csv"))
        df.glimpse()
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