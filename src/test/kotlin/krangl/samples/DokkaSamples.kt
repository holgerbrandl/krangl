package krangl.samples

import krangl.*
import java.time.LocalDate


enum class Sex { MALE, FEMALE }

fun selectExamples() {

    // data-frames can be a  mix of atomic (int, double, boolean, string) and object columns
    val birthdays = DataFrame.builder("name", "height", "sex", "birthday")(
            "Tom", 1.89, Sex.MALE, LocalDate.of(1980, 5, 22),
            "Jane", 1.73, Sex.FEMALE, LocalDate.of(1973, 2, 13)
    )

    // just select/deselect columns of interest with varargs
    birthdays.select("name", "height")
    birthdays.remove("sex")

    // use helper API to match columns of interest
    birthdays.select { matches("^na") }
    birthdays.select { endsWith("x") }

    // do positive and negative selection as needed
    birthdays.remove { startsWith("birth") }
    birthdays.select { except("birth") }


    // or use stdlib-like filter with `selectIf`
    birthdays.selectIf { it is IntCol }
    birthdays.selectIf { it.name.startsWith("bar") }
}

fun groupByExamples() {

    // group by a single attribute
    flightsData.groupBy("carrier")


    // or by multiple attributes
    flightsData.groupBy("carrier", "tailnum")

    // or by selecting grouping attriutes with indicator function (same as in `select()`
    flightsData.groupBy { startsWith("dep_") }

    // finally we can also group with arbitrary table expressions
    flightsData.groupByExpr { it["dep_time"] eq 22 }
    flightsData.groupByExpr({ it["dep_time"] eq 22 }, { it["carrier"] })
}


fun addColumnExamples() {

    flightsData.addColumn("delay_category") { df ->
        where(df["dep_delay"].asDoubles().mapNonNull { Math.abs(it) > 3 }.nullAsFalse(), "red", "green")
    }
}

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
            .filter { it["Species"].isMatching<String> { startsWith("se") } }
            .schema()
}


fun bindRowsExamples() {
    val places = dataFrameOf(
            "name", "population")(
            "Fort Joy", 150,
            "Cloudsdale", 2000
    )

    // You can add multiple rows at the same time.
    places.bindRows(
            mapOf("name" to "Tristram", "population" to 72),
            mapOf("name" to "Paper Town", "population" to 0)
    )

    // Adding incomplete rows inserts nulls
    places.bindRows(mapOf("population" to 10), mapOf("name" to "Paper Town"))

    // To drop additional columns originating from the bound rows simply select
    places.bindRows(mapOf("name" to "Bucklyn Cross", "area" to 52.2)).select(places.names)

    // Grouping is discarded when adding rows and needs to be reconstituated
    places.groupBy("name")
            .bindRows(mapOf("population" to 10))
            // regroup
            .groupBy("name")
}


fun iterableDeparsing() {

    val df = sleepPatterns.deparseRecords { sp ->
        mapOf(
                "awake" to sp.awake
        )
    }


    val df2 = sleepPatterns.deparseRecords(
            "foo" with { awake },
            "bar" with { it.brainwt?.plus(3) }
    )

    // or fully automatic using reflection
    val df3 = sleepPatterns.asDataFrame()
}

fun readExcelExample(){

    val colTypesMap: Map<String,ColType> = mapOf("Activities" to ColType.Int, "Email" to ColType.String)

    // Read excel file using sheet name
    var df = DataFrame.readExcel("src/test/resources/krangl/data/ExcelReadExample.xlsx", "FirstSheet")

    // Read excel file using sheet index
     df = DataFrame.readExcel("src/test/resources/krangl/data/ExcelReadExample.xlsx", 0)

    // Read excel file only in specified cell range
    df = DataFrame.readExcel("src/test/resources/krangl/data/ExcelReadExample.xlsx", 0, range = "A1:D10")

    // Read excel file without any leading or trailing whitespace
    df = DataFrame.readExcel("src/test/resources/krangl/data/ExcelReadExample.xlsx", 0, trim_ws = true)

    // Read excel file specifying column types (Remaining ones will be guessed)
    df = DataFrame.readExcel("src/test/resources/krangl/data/ExcelReadExample.xlsx", 0, colTypes = colTypesMap)

    // Set max number of records to analyze when guessing column type
    df = DataFrame.readExcel("src/test/resources/krangl/data/ExcelReadExample.xlsx", 0, guessMax = 10)
}

fun writeExcelExample(){

    // Creating an already populated DataFrame
    val df = DataFrame.readExcel("src/test/resources/krangl/data/ExcelReadExample.xlsx", 0)

    // Write to excel with default parameters (This will add a new sheet if the file exists)
    df.writeExcel("src/test/resources/krangl/data/ExcelWriteResult.xlsx", "FirstSheet")

    // Write to excel while completely overwriting an existing file (if it doesn't exist a new one is created)
    df.writeExcel("src/test/resources/krangl/data/ExcelWriteResult.xlsx", "FirstSheet", eraseFile = true)

    // Write to excel with normal headers (not bold)
    df.writeExcel("src/test/resources/krangl/data/ExcelWriteResult.xlsx", "SecondSheet", boldHeaders = false)

    // Write to excel without the column headers
    df.writeExcel("src/test/resources/krangl/data/ExcelWriteResult.xlsx", "ThirdSheet", headers = false)
}