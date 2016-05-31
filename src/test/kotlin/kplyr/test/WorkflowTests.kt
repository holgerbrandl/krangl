package kplyr.test

import io.kotlintest.specs.FlatSpec
import kplyr.*


class CompoundTests : FlatSpec() { init {

    "it" should "summarize sleep data" {

        val groupedSleep = sleepData.filter { it["awake"] gt 3 }.
                apply { glimpse() }.
                mutate("rem_proportion", { it["sleep_rem"] + it["sleep_rem"] }).
                groupBy("vore")


        val insectiMeanREM = sleepData.filter { it["awake"] gt 3 }.
                apply { glimpse() }.
                mutate("rem_proportion", { it["sleep_rem"] + it["sleep_rem"] }).
                groupBy("vore").
                summarize("mean_rem_prop", { it["rem_proportion"].mean(removeNA = true) }).
                filter { it["vore"] eq  "insecti" }.
                row(0)["mean_rem_prop"] as Double


        ((insectiMeanREM - 3.525) < 1E-5) shouldBe true
    }

    "it" should "allow to create dataframe in place"{
        // @formatter:off
        val df = (kplyr.dataFrameOf(
                "foo", "bar")) (
            "ll",   2,
            "sdfd", 4,
            "sdf",  5)
        //@formatter:on

        df.ncol shouldBe 2
        df.nrow shouldBe 3
        df.names shouldBe listOf("foo", "bar")

        val naDF = dataFrameOf(
                "foo", "bar") (
                null, null,
                "sdfd", null,
                "sdf", 5)

        (naDF["foo"] is StringCol) shouldBe true
        (naDF["bar"] is IntCol) shouldBe true

        naDF.summarize("num_na", { it["bar"].isNA().sumBy { if (it) 1 else 0 } }).print()
    }
}
}


class Playground : FlatSpec() { init {

    "it" should "allow to use different and multiple by columns"{
        UnequalByHelpers.joinInner(persons, weights, by = listOf("last_name" to "last")).apply {
            print()
            nrow shouldBe 2
            names shouldEqual listOf("last_name", "first_name", "age", "first", "weight")
        }
    }

}

}
