package krangl.test

import io.kotlintest.matchers.Matchers
import krangl.*
import org.junit.Test

/**
require(dplyr)
iris[1, "Species"] <- NA
head(iris)
group_by(iris, Species)
group_by(iris, Species) %>% summarize(mean_length=mean(Sepal.Width))
 */
class SpreadTest : Matchers {

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

class GatherTest : Matchers {

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
        wideDf.gather("property", "value", columns = { except("person") AND except("person") })

        wideDf.gather("property", "value", columns = -"person").apply {
            print()

            val annasSalary = filter { ((it["person"] eq "anna") AND (it["property"] eq "salary")) }
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


class SpreadUniteTest : Matchers {

    @Test
    fun `it should spread and unit columns`() {

        sleepData.unite("test", listOf("name", "order"), remove = false).apply {
            head().print()
            names.contains("name") shouldBe true
            this["test"].values().size shouldEqual nrow
        }

        sleepData.unite("test", { oneOf("name", "order") }).apply {
            head().print()

            names.contains("name") shouldBe false
            this["test"].values().size shouldEqual nrow
        }

        val united = sleepData.unite("test", { oneOf("name", "sleep_rem") })

        united.separate("test", listOf("new_name", "new_sleep_rem"), convert = true).apply {
            head().print()
            glimpse()

            this["new_name"] == sleepData["name"]
            this["new_sleep_rem"] == sleepData["sleep_rem"]
        }
    }
}
