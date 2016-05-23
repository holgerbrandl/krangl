package kplyr.test

import io.kotlintest.specs.FlatSpec
import kplyr.*


val sleepData = DataFrame.fromCSV(DataFrame::class.java.getResourceAsStream("data/msleep.csv"))

class CompoundTests : FlatSpec() { init {

    "it" should "summarize sleep data" {

        val groupedSleep = sleepData.filter { it["awake"] gt 3 }.
                apply { glimpse() }.
                mutate("rem_proportion", { it["sleep_rem"] + it["sleep_rem"] }).
                groupBy("vore")

        (groupedSleep as GroupedDataFrame).print()
        (groupedSleep as GroupedDataFrame).groups().print()

        val insectiMeanREM = sleepData.filter { it["awake"] gt 3 }.
                apply { glimpse() }.
                mutate("rem_proportion", { it["sleep_rem"] + it["sleep_rem"] }).
                groupBy("vore").
                summarize("mean_rem_prop", { it["rem_proportion"].mean(removeNA = true) }).
                filter { it["vore"] eq  "insecti" }.
                row(0)["mean_rem_prop"] as Double


        ((insectiMeanREM - 3.525) < 1E-5) shouldBe true
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


class GroupedDataTest : FlatSpec() { init {

    /** dplyr considers NA as a group and kplyr should do the same

    ```
    require(dplyr)

    iris
    iris$Species[1] <- NA

    ?group_by
    grpdIris <- group_by(iris, Species)
    grpdIris %>% slice(1)
    ```
     */
    "it" should "allow for NA as a group value" {
        // 1) test single attribute grouping with NA
        (sleepData.groupBy("vore") as GroupedDataFrame).groups().nrow shouldBe 6

        // 2) test multi-attribute grouping with NA in one or all attributes
//        (sleepData.groupBy("vore") as GroupedDataFrame).groups().nrow shouldBe 6
        //todo implement me
    }
}
}

