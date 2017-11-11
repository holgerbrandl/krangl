/**
 * @author Holger Brandl
 */


import krangl.*

// todo convert to script

/** Reproduce popular sleep data workflow with krangl*/


fun main(args: Array<String>) {
    // sleepData is bundles with Krangl
    sleepData.print()

    // but for sake of learning the API we load it from file here
    val sleepData  = DataFrame.fromCSV("/Users/brandl/projects/kotlin/krangl/src/main/resources/krangl/data/msleep.csv")

    // select columns of interest
    val slimSleep = sleepData.select("msleep", "name", "sleep_total")

    // Negative selection (aka column removal)
    sleepData.remove("conservation")


    // Do a range selection
    sleepData.select{ range("name", "order")}


    // Select all columns that start with the character string "sl" along with the `name` column, use the function `startsWith()`:
    sleepData.select({listOf("name")}, { startsWith("sl")})


    //
    // Filter rows with `filter`
    //

    // Find those animals that sleep more than 16h hour
    sleepData.filter { it["sleep_total"] gt 16} // which is a shortcut for:
    sleepData.filter { it["sleep_total"].greaterThan(16) }
}



