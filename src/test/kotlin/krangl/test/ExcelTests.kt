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

    @Test
    fun `readExcel -  should read excel file`() {

        val df = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
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
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            "FirstSheet"
        )

        val indexDF = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            0
        )

        nameDF shouldBe indexDF
    }

    @Test
    fun `readExcel - out of range test`() {
        val headerHigherThanContentDF = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            "FirstSheet", CellRangeAddress.valueOf("A105:A110")
        )

        headerHigherThanContentDF shouldBe emptyDataFrame()
    }

    @Test
    fun `readExcel - range test`() {
        val df = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            "FirstSheet"
        )

        // Test sheet by index + cell range
        val cellRangeTestDF = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            sheet = 1, cellRange = CellRangeAddress.valueOf("E5:J105"), trim = true
        )

        // Test defaulted cellRange's correctness on sheet with empty rows/cols
        val defaultCellRangeTestDF = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            sheet = 1, trim = true
        )

        cellRangeTestDF shouldBe df
        defaultCellRangeTestDF shouldBe cellRangeTestDF
    }

    @Test
    fun `readExcel - trim_ws should trim white space`() {
        val df = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            "FirstSheet"
        )
        val trimmedDF = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            sheet = 1, trim = true
        )

        trimmedDF shouldBe df
    }

    @Test
    fun `readExcel - colTypes should work`() {
        val df = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            "FirstSheet", colTypes = NamedColumnSpec("Activities" to ColType.Int)
        )

        (df["Activities"] is IntCol) shouldBe true // Tests when ColType is given
        (df["Registered"] is BooleanCol) shouldBe true // Tests when ColType is guessed
    }

    @Test
    fun `readExcel - should stop at first blank line`() {
        val shouldStopAtBlankDF = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            sheet = 2, trim = true, cellRange = CellRangeAddress.valueOf("E3:J10")
        )

        shouldStopAtBlankDF.nrow shouldBe 4
    }

    @Test
    fun `readExcel - should continue past blank line`() {
        val shouldContinueAtBlankDF = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            sheet = 2, trim = true, cellRange = CellRangeAddress.valueOf("E3:J10"), stopAtBlankLine = false
        )

        shouldContinueAtBlankDF.nrow shouldBe 6
    }

    @Test
    fun `readExcel - should include blank lines`() {
        val shouldContinueAtBlankDF = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
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
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            "FirstSheet"
        )

        df["Activities"][1] shouldBe 432178937489174
    }

    @Test
    fun `writeExcel - should write to excel`() {
        val df = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            "FirstSheet"
        )

        df.writeExcel(
            "src/test/resources/krangl/data/ExcelWriteResult.xlsx",
            "FirstSheet",
            headers = true,
            eraseFile = true,
            boldHeaders = false
        )
        df.writeExcel(
            "src/test/resources/krangl/data/ExcelWriteResult.xlsx",
            "SecondSheet",
            headers = true,
            eraseFile = false,
            boldHeaders = true
        )
        df.writeExcel(
            "src/test/resources/krangl/data/ExcelWriteResult.xlsx",
            "ThirdSheet",
            headers = false,
            eraseFile = false,
            boldHeaders = false
        )

        val writtenDF = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelWriteResult.xlsx",
            "FirstSheet"
        )

        writtenDF shouldBe df

        val writtenBook = XSSFWorkbook(FileInputStream("src/test/resources/krangl/data/ExcelWriteResult.xlsx"))
        val longValueCell = writtenBook.getSheet("FirstSheet").getRow(2).getCell(4)
        longValueCell.cellType.shouldBe(CellType.NUMERIC)
        longValueCell.numericCellValue.shouldBe(432178937489174.0)
        val emptyValueCell = writtenBook.getSheet("FirstSheet").getRow(6).getCell(4)
        emptyValueCell.cellType.shouldBe(CellType.BLANK)
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

        df[1][4] shouldBe null
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
}
