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

//    "it" should "reshape from wide to long" {
//    }


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

        longDf.spread("year", "weight").apply {
            nrow shouldBe 2
            ncol shouldBe 6  // name, sex, 4 year columns

            // ensure that types were coerced correctly
            (this["2013"] is DoubleCol) shouldBe  true
            (this["2016"] is DoubleCol) shouldBe  true
        }
    }


    "it" should "type convert stringified values from long to wide" {
        val longDf = dataFrameOf("person", "property", "value", "sex")(
                "max", "salary", "33.1", "M",
                "max", "city", "London", "M",
                "anna", "salary", "33.5", "F",
                "anna", "city", "berlin", "F"
        )

        longDf.spread("property", "value", convert = true).apply {
            nrow shouldBe 2
            ncol shouldBe 4  // name, sex, 4 year columns

            // ensure that types were coerced correctly
            (this["city"] is StringCol) shouldBe  true
            (this["salary"] is DoubleCol) shouldBe  true
        }
    }
}
}



infix fun String.to(that: DataFrame.(DataFrame) -> Any?) = TableFormula(this, that)


//@Suppress("UNCHECKED_CAST")
//internal fun coerceColumnFromAny(name: String, value: Array<*>) = when {
//    value.find { it is Int } != null -> IntCol(name, Array(value.size, { index -> value[index] as Int? }))
//    value.find { it is Double } != null -> DoubleCol(name, Array(value.size, { index -> value[index] as Double? }))
//    value.find { it is String } != null -> StringCol(name, Array(value.size, { index -> value[index] as String? }))
//    value.find { it is Boolean } != null -> BooleanCol(name, Array(value.size, { index -> value[index] as Boolean? }))
//    else -> AnyCol(name, Array(value.size, { index -> value[index] as Any? }))
//}
