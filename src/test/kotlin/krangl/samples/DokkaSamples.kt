package krangl.samples

import krangl.*
import java.time.LocalDate


enum class Sex { MALE, FEMALE }

fun builderSample() {
    // data-frames can be a  mix of atomic (int, double, boolean, string) and object columns
    val birthdays = DataFrame.builder("name", "height", "sex", "birthday")(
        "Tom", 1.89, Sex.MALE, LocalDate.of(1980, 5, 22),
        "Jane", 1.73, Sex.FEMALE, LocalDate.of(1973, 2, 13)
    )
}


fun packageInfoSample() {
    flightsData
        .groupBy("year", "month", "day")
        .select({ range("year", "day") }, { listOf("arr_delay", "dep_delay") })
        .summarize(
            "mean_arr_delay" to { it["arr_delay"].mean(removeNA = true) },
            "mean_dep_delay" to { it["dep_delay"].mean(removeNA = true) }
        )
        .filter { (it["mean_arr_delay"] gt 30) OR (it["mean_dep_delay"] gt 30) }
}


fun renameSomeColumns() {
    // note: this is a bit of the scope of the current API and also no typical SQL op.

    // manual way: first select column names to be altered
    irisData.names.filter { it.startsWith("Sepal") }
        // second, apply renaming
        .fold(irisData, { df, colName -> df.rename(colName to "My" + colName) })


    // or refactor it away by defining an extension functino to do it
    fun DataFrame.renameSelect(colSelector: String.() -> Boolean, renamingRule: (String) -> String): DataFrame = names
        .filter(colSelector)
        .fold(this, { df, colName -> df.rename(colName to renamingRule(colName)) })

    // and use it like
    irisData.renameSelect({ startsWith("Sepal") }, { "My" + it })

}

fun textMatching() {
    irisData
        // filter for all record where species starts with "se"
        .filter { it["Species"].isMatching { startsWith("se") } }
        .glimpse()
}