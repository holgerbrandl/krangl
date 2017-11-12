package krangl.samples

import krangl.DataFrame
import krangl.*
import krangl.flightsData
import java.time.LocalDate


enum class Sex{MALE, FEMALE}

fun builderSample(){
    // data-frames can be a  mix of atomic (int, double, boolean, string) and object columns
    val birthdays = DataFrame.builder("name", "height", "sex", "birthday")(
            "Tom", 1.89, Sex.MALE, LocalDate.of(1980, 5,22),
            "Jane", 1.73, Sex.FEMALE, LocalDate.of(1973, 2,13)
    )
}


fun packageInfoSample(){
    flightsData
        .groupBy("year", "month", "day")
        .select({ range("year", "day") }, { listOf("arr_delay", "dep_delay") })
        .summarize(
            "mean_arr_delay" to { it["arr_delay"].mean(removeNA = true) },
            "mean_dep_delay" to { it["dep_delay"].mean(removeNA = true) }
        )
        .filter { (it["mean_arr_delay"] gt  30)  OR  (it["mean_dep_delay"] gt  30) }
}