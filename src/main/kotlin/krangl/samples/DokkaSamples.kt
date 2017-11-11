package krangl.samples

import krangl.DataFrame
import krangl.builder
import java.time.LocalDate

/**
 * @author Holger Brandl
 */

enum class Sex{MALE, FEMALE}

fun buildDataFrameof(){

    // data-frames can ne a mix o atomic  (int, double, boolean) and object columns
    val birthdays = DataFrame.builder("name", "height", "sex", "birthday")(
            "Tom", 1.89, Sex.MALE, LocalDate.of(1980, 5,22),
            "Jane", 1.73, Sex.FEMALE, LocalDate.of(1973, 2,13)
    )

}