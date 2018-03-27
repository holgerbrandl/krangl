package krangl.test

import io.kotlintest.matchers.shouldBe
import io.kotlintest.matchers.shouldEqual
import krangl.*
import org.junit.Test

/**
require(dplyr)
iris[1, "Species"] <- NA
head(iris)
group_by(iris, Species)
group_by(iris, Species) %>% summarize(mean_length=mean(Sepal.Width))
 */
class SpreadTest {

    @Test
    fun `it should reshape from long to wide`() {
        val longDf = dataFrameOf("person", "year", "weight", "sex")(
            "max", 2014, 33.1, "M",
            "max", 2015, 32.3, "M",
            "max", 2016, null, "M",
            "anna", 2013, 33.5, "F",
            "anna", 2014, 37.3, "F",
            "anna", 2015, 39.2, "F",
            "anna", 2016, 39.9, "F"
        )

        longDf.spread("year", "weight").apply {
            nrow shouldBe 2
            ncol shouldBe 6  // name, sex, 4 year columns

            // ensure that types were coerced correctly
            (this["2013"] is DoubleCol) shouldBe true
            (this["2016"] is DoubleCol) shouldBe true
        }
    }


    @Test
    fun `it should type convert stringified values from long to wide`() {
        val longDf = dataFrameOf("person", "property", "value", "sex")(
            "max", "salary", "33.1", "M",
            "max", "city", "London", "M",
            "anna", "salary", "33.5", "F",
            "anna", "city", "Berlin", "F"
        )

        longDf.spread("property", "value", convert = true).apply {
            nrow shouldBe 2
            ncol shouldBe 4  // name, sex, 4 year columns

            // ensure that types were coerced correctly
            (this["city"] is StringCol) shouldBe true
            (this["salary"] is DoubleCol) shouldBe true
        }
    }
}

class GatherTest {

    @Test
    fun `it should reshape from wide to long`() {
        //        val longDf = dataFrameOf("person", "year", "weight", "sex")(
        //                "max", 2014, 33.1, "M",
        //                "max", 2015, 32.3, "M",
        //                "max", 2016, null, "M",
        //                "anna", 2013, 33.5, "F",
        //                "anna", 2014, 37.3, "F",
        //                "anna", 2015, 39.2, "F",
        //                "anna", 2016, 39.9, "F"
        //        )
        //
        //        longDf.spread("year", "weight").apply {
        //            nrow shouldBe 2
        //            ncol shouldBe 6  // name, sex, 4 year columns
        //
        //            // ensure that types were coerced correctly
        //            (this["2013"] is DoubleCol) shouldBe  true
        //            (this["2016"] is DoubleCol) shouldBe  true
        //        }
    }

    val longDf = dataFrameOf("person", "property", "value", "sex")(
        "max", "salary", "33.1", "M",
        "max", "city", "London", "M",
        "anna", "salary", "33.5", "F",
        "anna", "city", "Berlin", "F"
    )

    val wideDf = longDf.spread("property", "value")


    @Test
    fun `it should allow to exclude key columns from gathering`() {
        // todo that's illegal because it's mixing positive and negative selection
        wideDf.gather("property", "value", columns = { except("person") AND startsWith("person") })

        wideDf.gather("property", "value", columns = { except("person") })

        wideDf.gather("property", "value", columns = listOf("person"))

        wideDf.gather("property", "value", columns = { except("person") }).apply {
            print()

            val annasSalary = filter { (it["person"] eq "anna") AND (it["property"] eq "salary") }
            annasSalary["value"].values().first() shouldBe "33.5"
        }
    }


    @Test
    fun `it should maintain spread gather equality`() {
        val longAgain: DataFrame = wideDf.gather("property", "value")

        longAgain.let {
            it == longDf // this also tests equals for DataFrame
            it.hashCode() == longDf.hashCode()  // and for sake of completeness also test hashCode here
        }

    }
}


class SpreadUniteTest {

    @Test
    fun `it should spread and unit columns`() {

        sleepData.unite("test", listOf("name", "order"), remove = false).apply {
            take().print()
            names.contains("name") shouldBe true
            this["test"].values().size shouldEqual nrow
        }

        sleepData.unite("test", { listOf("name", "order") }).apply {
            take().print()

            names.contains("name") shouldBe false
            this["test"].values().size shouldEqual nrow
        }

        val united = sleepData.unite("test", { listOf("name", "sleep_rem") }, sep = ",")

        united.separate("test", listOf("new_name", "new_sleep_rem"), convert = true, sep = ",").apply {
            take().print()
            schema()

            this["new_name"] == sleepData["name"]
            this["new_sleep_rem"] == sleepData["sleep_rem"]
        }
    }
}


class NestingTests {

    @Test
    fun `it nest grouped data`() {
        irisData
            .groupBy("Species")
            .nest()
            .apply {
                nrow shouldBe 3
                ncol shouldBe 2
                names shouldEqual listOf("Species", "data")

                // also make sure that output looks good
                //                captureOutput { print() }.stdout shouldBe """
                //                A DataFrame: 3 x 2
                //                   Species                   data
                //                    setosa   <DataFrame [50 x 4]>
                //                versicolor   <DataFrame [50 x 4]>
                //                 virginica   <DataFrame [50 x 4]>
                //                    """.trimIndent()

                captureOutput { schema() }.stdout shouldBe """
                    DataFrame with 3 observations
                    Species  [Str]        setosa, versicolor, virginica
                    data     [DataFrame]  <DataFrame [50 x 4]>, <DataFrame [50 x 4]>, <DataFrame [50 x 4]>
                    """.trimIndent()
            }
    }

    @Test
    fun `it nest ungrouped data`() {
        irisData
            .nest()
            .apply {
                nrow shouldBe 1
                ncol shouldBe 1
                names shouldEqual listOf("data")
            }
    }

    @Test
    fun `it nest selected columns only`() {
        irisData
            .nest({ except("Species") })
            .apply {
                schema()
                nrow shouldBe 3
                ncol shouldBe 2
                names shouldEqual listOf("Species", "data")
            }
    }


    @Test
    fun `it should unnest data`() {
        // use other small but NA-heavy data set here
        val restored = sleepData
            .nest({ except("order") })
            .unnest()
            .sortedBy("order")
            .moveLeft("name", "genus", "vore")



        restored.apply {
            print()

            nrow shouldBe sleepData.nrow
            ncol shouldBe sleepData.ncol
            names shouldBe sleepData.names
        }

        restored shouldEqual sleepData.sortedBy("order")
    }
}