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

        val wideDf = longDf.spread("year", "weight").apply {
            nrow shouldBe 2
            ncol shouldBe 6  // name, sex, 4 year columns

            // ensure that types were coerced correctly
            (this["2013"] is DoubleCol) shouldBe  true
            (this["2016"] is DoubleCol) shouldBe  true
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
// todo try to convert types if possible
fun DataFrame.spread(key: String, value: String, fill: Any? = null): DataFrame {
    // create new columns
    val newColNames = this[key].values().distinct()  // .map { it.toString() } dont'convert already because otherwise join will fail

    // make sure that new column names do not exist already
    require(names.intersect(newColNames).isEmpty()) { "spread columns do already exist in data-frame" }

    // todo use big initially empty array here and fill it with spread data

    val bySpreadGroup = groupBy(*names.minus(listOf(key, value)).toTypedArray()) as GroupedDataFrame

    val spreadGroups: List<DataFrame> = bySpreadGroup
            .groups
            .map {
                val grpDf = it.df

                require(grpDf.select(key).distinct(key).nrow == grpDf.nrow) { "key value mapping is not unique" }
//
//                val spreadBlock = SimpleDataFrame(StringCol(key, newColNames)).leftJoin(grpDf.select(key, value))
                val spreadBlock = SimpleDataFrame(handleListErasure(key, newColNames)).leftJoin(grpDf.select(key, value))


                val grpSpread = SimpleDataFrame((spreadBlock as SimpleDataFrame).rows.map {
//                    // too basic: simply use any column
                    AnyCol(it[key].toString(), listOf(it[value]))

                    // better preserve type for new column
//                    coerceColumnFromAny(it[key].toString(), it[value])
                })

                bindCols(grpDf.select(-key, -value).distinct(), grpSpread)
            }

//    if(fill!=null){
//        spreadBlock =  spreadBlock.
//    }

    val spreadWithGHashes = spreadGroups.bindRows()


    // coerce types of strinified coluymns similar to how tidy is doing things
    val typeConvSpread = newColNames.map { it.toString() }
            .foldRight(spreadWithGHashes, { spreadCol, df ->
                df.mutate(spreadCol to { coerceColumnFromAny(spreadCol, df[spreadCol].values()) })
            })


    return typeConvSpread
}

infix fun String.to(that: DataFrame.(DataFrame) -> Any?) = TableFormula(this, that)


@Suppress("UNCHECKED_CAST")
internal fun coerceColumnFromAny(name: String, value: Array<*>) = when {
    value.find { it is Int } != null -> IntCol(name, Array(value.size, { index -> value[index] as Int? }))
    value.find { it is Double } != null -> DoubleCol(name, Array(value.size, { index -> value[index] as Double? }))
    value.find { it is String } != null -> StringCol(name, Array(value.size, { index -> value[index] as String? }))
    value.find { it is Boolean } != null -> BooleanCol(name, Array(value.size, { index -> value[index] as Boolean? }))
    else -> AnyCol(name, Array(value.size, { index -> value[index] as Any? }))
}
