package krangl.test

import io.kotlintest.specs.FlatSpec
import krangl.*


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
        val df = (krangl.dataFrameOf(
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
    "it" should "test something"{
//        sleepData.rename("vore" to "vore").names shouldBe sleepData.names
        UnequalByHelpers.innerJoin(sleepData, sleepData.rename("order" to "new_order"), by = listOf(
                "vore" to "vore",
                "order" to "new_order"
        )).nrow shouldBe 597
    }

//    "it" should "allow to use different and multiple by columns"{
//        innerJoin(df, df, by = emptyList(), suffices = "_1" to "_2").apply {
//            nrow shouldBe 0
//            names shouldEqual  listOf("foo_1", "bar_1", "foo_2", "bar_2")
//        }
//    }

//    "it" should "allow to use different and multiple by columns"({
//        innerJoin(persons.rename("last_name" to "name"), weights, by = listOf("name" to "last")).apply {
//            nrow shouldBe 2
//        }
//    })

}

}
