package krangl.test

import io.kotest.matchers.shouldBe
import krangl.*
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.ss.util.CellRangeAddress
import org.apache.poi.util.LocaleUtil
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.junit.Test
import java.io.FileInputStream
import java.util.*


class ExcelTests {

    private val testReadingPath = "src/test/resources/krangl/data/ExcelReadExample.xlsx"
    private val testWritingPath = "src/test/resources/krangl/data/ExcelWriteResult.xlsx"


    @Test
    fun `readExcel -  should read excel file`() {

        val df = DataFrame.readExcel(
            testReadingPath,
            "FirstSheet"
        )

        df.names shouldBe listOf("Name", "Email", "BirthDate", "Country", "Activities", "Registered")
        df.nrow shouldBe 100
        df["Name"][0] shouldBe "Hyatt"
        df.print(title = "ExcelReadTest")
        println(df.schema())
    }

    @Test
    fun `readExcel - sheet by name should match sheet by index`() {
        val nameDF = DataFrame.readExcel(
            testReadingPath,
            "FirstSheet"
        )

        val indexDF = DataFrame.readExcel(
            testReadingPath,
            0
        )

        nameDF shouldBe indexDF
    }

    @Test
    fun `readExcel - out of range test`() {
        val headerHigherThanContentDF = DataFrame.readExcel(
            testReadingPath,
            "FirstSheet", CellRangeAddress.valueOf("A105:A110")
        )

        headerHigherThanContentDF shouldBe emptyDataFrame()
    }

    @Test
    fun `readExcel - range test`() {
        val df = DataFrame.readExcel(
            testReadingPath,
            "FirstSheet"
        )

        // Test sheet by index + cell range
        val cellRangeTestDF = DataFrame.readExcel(
            testReadingPath,
            sheet = 1, cellRange = CellRangeAddress.valueOf("E5:J105"), trim = true
        )

        // Test defaulted cellRange's correctness on sheet with empty rows/cols
        val defaultCellRangeTestDF = DataFrame.readExcel(
            testReadingPath,
            sheet = 1, trim = true
        )

        cellRangeTestDF shouldBe df
        defaultCellRangeTestDF shouldBe cellRangeTestDF
    }

    @Test
    fun `readExcel - trim_ws should trim white space`() {
        val df = DataFrame.readExcel(
            testReadingPath,
            "FirstSheet"
        )
        val trimmedDF = DataFrame.readExcel(
            testReadingPath,
            sheet = 1, trim = true
        )

        trimmedDF shouldBe df
    }

    @Test
    fun `readExcel - colTypes should work`() {
        val df = DataFrame.readExcel(
            testReadingPath,
            "FirstSheet", colTypes = NamedColumnSpec("Activities" to ColType.Int)
        )

        (df["Activities"] is IntCol) shouldBe true // Tests when ColType is given
        (df["Registered"] is BooleanCol) shouldBe true // Tests when ColType is guessed
    }

    @Test
    fun `readExcel - should stop at first blank line`() {
        val shouldStopAtBlankDF = DataFrame.readExcel(
            testReadingPath,
            sheet = 2, trim = true, cellRange = CellRangeAddress.valueOf("E3:J10")
        )

        shouldStopAtBlankDF.nrow shouldBe 4
    }

    @Test
    fun `readExcel - should continue past blank line`() {
        val shouldContinueAtBlankDF = DataFrame.readExcel(
            testReadingPath,
            sheet = 2, trim = true, cellRange = CellRangeAddress.valueOf("E3:J10"), stopAtBlankLine = false
        )

        shouldContinueAtBlankDF.nrow shouldBe 6
    }

    @Test
    fun `readExcel - should include blank lines`() {
        val shouldContinueAtBlankDF = DataFrame.readExcel(
            testReadingPath,
            sheet = 2,
            trim = true,
            cellRange = CellRangeAddress.valueOf("E3:J10"),
            stopAtBlankLine = false,
            includeBlankLines = true
        )

        shouldContinueAtBlankDF.nrow shouldBe 7
    }

    @Test
    fun `readExcel - should read bigint value`() {

        val df = DataFrame.readExcel(
            testReadingPath,
            "FirstSheet"
        )

        df["Activities"][1] shouldBe 432178937489174
    }

    @Test
    fun `writeExcel - should write to excel`() {
        val df = DataFrame.readExcel(
            testReadingPath,
            "FirstSheet"
        )

        df.writeExcel(
            testWritingPath,
            "FirstSheet",
            headers = true,
            eraseFile = true,
            boldHeaders = false
        )
        df.writeExcel(
            testWritingPath,
            "SecondSheet",
            headers = true,
            eraseFile = false,
            boldHeaders = true
        )
        df.writeExcel(
            testWritingPath,
            "ThirdSheet",
            headers = false,
            eraseFile = false,
            boldHeaders = false
        )

        val writtenDF = DataFrame.readExcel(
            testWritingPath,
            "FirstSheet"
        )

        writtenDF shouldBe df

        val writtenBook = XSSFWorkbook(FileInputStream(testWritingPath))
        val longValueCell = writtenBook.getSheet("FirstSheet").getRow(2).getCell(4)
        longValueCell.cellType.shouldBe(CellType.NUMERIC)
        longValueCell.numericCellValue.shouldBe(432178937489174.0)
        val emptyValueCell = writtenBook.getSheet("FirstSheet").getRow(6).getCell(4)
        emptyValueCell shouldBe null
    }

    @Test
    // note: we aim for consistency with  read_excel("missing_data.xlsx", na="NA")
    fun `it should correctly convert NA and empty cells to NA`() {
        val df = DataFrame.readExcel(
            "src/test/resources/krangl/data/missing_data.xlsx",
            na = "NA",
            colTypes = NamedColumnSpec("Registered" to ColType.Boolean)
        )

        df.print(maxWidth = 1000)
        df.schema()

        df[1][4] shouldBe ""
        df[3][1] shouldBe null
        df[5][3] shouldBe null

        // also make sure that it parsed the boolean column as such
        (df.cols[5] is BooleanCol) shouldBe true
    }

    @Test
    fun `it should honor the system local when reading xls`() {
        val defaultLocale = LocaleUtil.getUserLocale()
        listOf(Locale.GERMAN, Locale.FRENCH, Locale.ENGLISH).forEach { locale ->
            LocaleUtil.setUserLocale(locale)

            val df = DataFrame.readExcel(
                "src/test/resources/krangl/data/locale_test.xlsx",
                "Operation"
            )
            df.print()
            df.schema()

            (df.cols[2] is IntCol) shouldBe true
            (df.cols[3] is DoubleCol) shouldBe true
        }

        LocaleUtil.setUserLocale(defaultLocale)
    }

    @Test
    fun `it should distinguish empty strings and nulls`() {
        val df1 = dataFrameOf(listOf(
            mapOf("col" to "NotEmptyString1", "col2" to 1),
            mapOf("col" to "", "col2" to 2),
            mapOf("col" to null, "col2" to 3),
            mapOf("col" to "NotEmptyString2", "col2" to 4),
        ))
        df1.writeExcel(testWritingPath, "test", eraseFile = true)
        DataFrame.readExcel(testWritingPath, "test") shouldBe df1

        val df2 = dataFrameOf(listOf(
            mapOf("col" to "NotEmptyString1"),
            mapOf("col" to ""),
            mapOf("col" to null),
            mapOf("col" to "NotEmptyString2"),
        ))
        df2.writeExcel("testNull.xlsx", "test", eraseFile = true)
        DataFrame.readExcel("testNull.xlsx", "test", stopAtBlankLine = false, includeBlankLines = true) shouldBe df2
    }
}
