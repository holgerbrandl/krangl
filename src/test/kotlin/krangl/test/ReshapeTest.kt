package krangl.test

import io.kotlintest.specs.FlatSpec
import krangl.*

/**
require(dplyr)
iris[1, "Species"] <- NA
head(iris)
group_by(iris, Species)
group_by(iris, Species) %>% summarize(mean_length=mean(Sepal.Width))

 */
class ReshapeTest : FlatSpec() { init {

    "it" should "reshape from wide to long" {
    }


    "it" should "reshape from long to wide" {
        val longDf = dataFrameOf("person", "year", "weight", "sex")(
                "max", 2014, 33.1, "M",
                "max", 2015, 32.3, "M",
                "max", 2016, null, "M",
                "anna", 2013, 33.5, "F",
                "anna", 2014, 37.3, "F",
                "anna", 2015, 39.2, "F",
                "anna", 2016, 39.9, "F"
        )

        val wideDf = longDf.spread("year", "weight").apply {
            nrow shouldBe 2
            ncol shouldBe 6  // name, sex, 4 year columns
        }
    }
}
}


/**
 * Spread a key-value pair across multiple columns.
 *
 * @param key The bare (unquoted) name of the column whose values will be used as column headings.
 * @param value The bare (unquoted) name of the column whose values will populate the cells.
 * @param fill If set, missing values will be replaced with this value.
 */
fun DataFrame.spread(key: String, value: String, fill: Any? = null): DataFrame {
    // create new columns
    val newColNames = this[key].values().distinct()

    // make sure that new column names do not exist already
    require(names.intersect(newColNames).isEmpty()) { "spread columns do already exist in data-frame" }

    val spreadBlock = dataFrameOf(key)(newColNames).outerJoin(this.select(key, value))
    return this.leftJoin(spreadBlock, by = "key")
}

