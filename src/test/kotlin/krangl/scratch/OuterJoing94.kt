package krangl.scratch

import krangl.dataFrameOf
import krangl.outerJoin
import krangl.print

fun main() {
    val user = dataFrameOf(
        "first_name", "last_name", "age", "weight"
    )(
        "Max", "Doe", 23, 55,
        "Franz", "Smith", 23, 88,
        "Horst", "Keanes", 12, 82
    )

    val pets = dataFrameOf("first_name", "pet")(
        "Max", "Cat",
        "Franz", "Dog",
        // no pet for Horst
        "Uwe", "Elephant" // Uwe is not in user dataframe
    )

    pets.outerJoin(user).print("outer1")
    user.outerJoin(pets).print("outer2")
}