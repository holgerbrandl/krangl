package krangl.test

import io.kotlintest.matchers.plusOrMinus
import io.kotlintest.matchers.shouldBe
import krangl.*
import org.junit.Test


fun main(args: Array<String>) {
    val foo = sleepData
        //        .filter { it["awake"] gt 3 }
        //        .addColumn("rem_proportion", { it["sleep_rem"] / it["sleep_total"] })
        .filter { it["vore"] eq "insecti" }

    foo.glimpse()
    print(foo)
}

class CompoundTests {

    @Test
    fun `it should summarize sleep data`() {

        val groupedSleep = sleepData
            .filter { it["awake"] gt 3 }
            .apply { glimpse() }
            .addColumn("rem_proportion", { it["sleep_rem"] + it["sleep_rem"] })
            .groupBy("vore")


        val meanRemPropInsecti = sleepData
            .filter { it["awake"] gt 3 }
            //            .apply { glimpse() }
            .addColumn("rem_proportion", { it["sleep_rem"] / it["sleep_total"] })
            .filter { it["vore"] eq "insecti" }
            .groupBy("vore")
            .summarize("mean_rem_prop", { it["rem_proportion"].mean(removeNA = true) })
            .filter { it["vore"] eq "insecti" }.row(0)["mean_rem_prop"] as Double


        meanRemPropInsecti shouldBe (0.221 plusOrMinus 3.2)
    }

    @Test
    fun `it should allow to create dataframe in place`() {
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
            "foo", "bar")(
            null, null,
            "sdfd", null,
            "sdf", 5)

        (naDF["foo"] is StringCol) shouldBe true
        (naDF["bar"] is IntCol) shouldBe true

        naDF.summarize("num_na", { it["bar"].isNA().sumBy { if (it) 1 else 0 } }).print()
    }
}


class TestPlayground {

    @Test
    fun `it should test something`() {


    }
}
