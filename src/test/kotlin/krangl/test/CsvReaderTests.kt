package krangl.test

import io.kotest.assertions.withClue
import io.kotest.data.headers
import io.kotest.data.row
import io.kotest.data.table
import io.kotest.matchers.shouldBe
import krangl.*
import org.apache.commons.csv.CSVFormat
import org.junit.Test
import java.io.*
import kotlin.io.path.createTempFile


class CsvReaderTests {

    private val headerlessCsv = File("src/test/resources/krangl/data/headerless_with_preamble.txt")

    private val sleepDataShape =
        DfShape(
            rowCount = 83,
            colCount = 11,
            colNames = listOf(
                "name",
                "genus",
                "vore",
                "order",
                "conservation",
                "sleep_total",
                "sleep_rem",
                "sleep_cycle",
                "awake",
                "brainwt",
                "bodywt"
            )
        )

    private val testHeaderTypesCsv = File("src/test/resources/krangl/data/test_header_types.csv")
    private val testHeaderTypesShape =
        DfShape(
            rowCount = 7,
            colCount = 6,
            colNames = listOf("a", "b", "c", "d", "e", "f")
        )


    private val tornandoCsv = File("src/test/resources/krangl/data/1950-2014_torn.csv")
    private val tornandoShape =
        DfShape(
            rowCount = 59945,
            colCount = 28,
            colNames = listOf(
                "Number",
                "Year",
                "Month",
                "Day",
                "Date",
                "Time",
                "Zone",
                "State",
                "State FIPS",
                "State No",
                "Scale",
                "Injuries",
                "Fatalities",
                "Loss",
                "Crop Loss",
                "Start Lat",
                "Start Lon",
                "End Lat",
                "End Lon",
                "Length",
                "Width",
                "NS",
                "SN",
                "SG",
                "FIPS 1",
                "FIPS 2",
                "FIPS 3",
                "FIPS 4"
            )
        )

    @Test
    fun `readDelim(Reader)`() =
        DataFrame.readDelim(FileReader(testHeaderTypesCsv))
            .shouldHave(testHeaderTypesShape)

    @Test
    fun `readDelim(InputStream)`() =
        DataFrame.readDelim(testHeaderTypesCsv.toURL().openStream())
            .shouldHave(testHeaderTypesShape)

    @Test
    fun `readDelim(URI)`() =
        DataFrame.readDelim(testHeaderTypesCsv.toURI())
            .shouldHave(testHeaderTypesShape)

    @Test
    fun `readDelim(http)`() =
        LocalServer("/" to testHeaderTypesCsv).use { server ->
            DataFrame.readCSV("HttP://localhost:${server.port}")
                .shouldHave(testHeaderTypesShape)
        }

    @Test
    fun `readDelim(Reader, Skip)`() =
        DataFrame.readDelim(FileReader(headerlessCsv), skip = 6, format = CSVFormat.TDF)
            .shouldHave(DfShape(rowCount = 14, colCount = 3, colNames = listOf("X1", "X2", "X3")))


    @Test
    fun `readCSV(File)`() =
        DataFrame.readCSV(tornandoCsv).shouldHave(tornandoShape)

    @Test
    fun `readCSV(fileName)`() =
        DataFrame.readCSV(tornandoCsv.absolutePath).shouldHave(tornandoShape)

    @Test
    fun `readCSV(http)`() =
        LocalServer("/" to tornandoCsv).use { server ->
            DataFrame.readCSV("HttP://localhost:${server.port}")
                .shouldHave(tornandoShape)

        }

    @Test
    fun `readCSV(text) should read writeCSV(text)`() {

        createTempFile(prefix = "krangl_test", suffix = ".txt").let { path ->
            val file = path.toFile()
            sleepData.writeCSV(file, format = CSVFormat.TDF.withHeader())

            DataFrame.readCSV(file, format = CSVFormat.TDF.withHeader())
                .shouldHave(sleepDataShape)
        }
    }

    @Test
    fun `readCSV(zipped) should read writeCSV(zipped)`() {

        createTempFile(prefix = "krangl_test", suffix = ".zip").let { path ->
            val file = path.toFile()
            sleepData.writeCSV(file, format = CSVFormat.TDF.withHeader())

            DataFrame.readCSV(file, format = CSVFormat.TDF.withHeader())
                .shouldHave(sleepDataShape)
        }
    }

    @Test
    fun `it should have the correct column types`() {

        val columnTypes = NamedColumnSpec(
            "a" to ColType.String,
            "b" to ColType.String,
            "c" to ColType.Double,
            "e" to ColType.Boolean,
            "f" to ColType.Long,
            ".default" to ColType.Int

        )

        val dataFrame = DataFrame.readCSV(testHeaderTypesCsv, colTypes = columnTypes)
        val cols = dataFrame.cols
        assert(cols[0] is StringCol)
        assert(cols[1] is StringCol)
        assert(cols[2] is DoubleCol)
        assert(cols[3] is IntCol)
        assert(cols[4] is BooleanCol)
        assert(cols[5] is LongCol)
    }

    @Test
    fun `it should read csv with compact column spec`() {
        val dataFrame = DataFrame.readCSV(testHeaderTypesCsv, colTypes = CompactColumnSpec("s?dibl"))

        val cols = dataFrame.cols
        assert(cols[0] is StringCol)
        assert(cols[1] is StringCol)
        assert(cols[2] is DoubleCol)
        assert(cols[3] is IntCol)
        assert(cols[4] is BooleanCol)
        assert(cols[5] is LongCol)
    }

    val customNaDataFrame by lazy {
        DataFrame.readCSV(
            "src/test/resources/krangl/data/custom_na_value.csv",
            format = CSVFormat.DEFAULT.withHeader().withNullString("CUSTOM_NA")
        )
    }


    @Test
    fun `it should have a custom NA value`() {
        val cols = customNaDataFrame.cols
        assert(cols[0][0] == null)
    }

    @Test
    fun `it should peek until it hits the first N non NA values and guess IntCol`() {
        val cols = customNaDataFrame.cols
        assert(cols[0] is IntCol)

    }

    @Test
    fun `it should read fixed-delim data`() {
        // references
        //https://issues.apache.org/jira/browse/CSV-272
        //https://github.com/GuiaBolso/fixed-length-file-handler
        //https://dev.to/leocolman/handling-fixed-length-files-using-a-kotlin-dsl-1hm1

        val content = """
1         Product 1    Pa wafer                 7.28571        25
2         Product 2    Pb wafer                 NA             25
3         Product 3    test wafer               0.42857        25
    """.trim().trimIndent()


        val format = listOf(
            "Process Flow" to 10,
            "Product ID" to 10,
            "Product Name" to 25,
            "Start Rate" to 10,
            "Lot Size" to 10
        )
        val df = DataFrame.readFixedWidth(StringReader(content), format)
        df.print()
        df.schema()

        df.ncol shouldBe format.size
        df["Start Rate"][1] shouldBe null
    }


    // Should be replaced by some sort of Schema type
    private data class DfShape(val rowCount: Int, val colCount: Int, val colNames: List<String>)

    private fun DataFrame.shouldHave(dfShape: DfShape) {
        this.apply {
            withClue("Unexpected rowCount") { nrow shouldBe dfShape.rowCount }
            withClue("Unexpected colCount") { ncol shouldBe dfShape.colCount }
            withClue("Unexpected colNames") { names shouldBe dfShape.colNames }
        }
    }

}
