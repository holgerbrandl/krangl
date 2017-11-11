/**
 * @author Holger Brandl
 */


import krangl.*

// todo convert to script

fun main(args: Array<String>) {
    // sleepData is bundles with Krangl
    sleepData.print()

    // or load it from file
    val sleepData  = DataFrame.fromCSV("/Users/brandl/projects/kotlin/krangl/src/main/resources/krangl/data/msleep.csv")
}


