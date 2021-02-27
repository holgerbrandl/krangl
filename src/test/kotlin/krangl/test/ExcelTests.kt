package krangl.test

import io.kotest.matchers.shouldBe
import krangl.*
import org.apache.poi.ss.util.CellRangeAddress
import org.junit.Test

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
            sheet = 1, cellRange = CellRangeAddress.valueOf("E5:J105"), trim_ws = true
        )

        // Test defaulted cellRange's correctness on sheet with empty rows/cols
        val defaultCellRangeTestDF = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            sheet = 1, trim_ws = true
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
            sheet = 1, trim_ws = true
        )

        trimmedDF shouldBe df
    }

    @Test
    fun `readExcel - colTypes should work`() {
        val df = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            "FirstSheet", colTypes = mapOf(Pair("Activities", ColType.Int))
        )

        (df["Activities"] is IntCol) shouldBe true // Tests when ColType is given
        (df["Registered"] is BooleanCol) shouldBe true // Tests when ColType is guessed
    }

    @Test
    fun `readExcel - should stop at first blank line`() {
        val shouldStopAtBlankDF = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            sheet = 2, trim_ws = true, cellRange = CellRangeAddress.valueOf("E3:J10")
        )

        shouldStopAtBlankDF.nrow shouldBe 4
    }

    @Test
    fun `readExcel - should continue past blank line`() {
        val shouldContinueAtBlankDF = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            sheet = 2, trim_ws = true, cellRange = CellRangeAddress.valueOf("E3:J10"), stopAtBlankLine = false
        )

        shouldContinueAtBlankDF.nrow shouldBe 6
    }

    @Test
    fun `readExcel - should include blank lines`() {
        val shouldContinueAtBlankDF = DataFrame.readExcel(
            "src/test/resources/krangl/data/ExcelReadExample.xlsx",
            sheet = 2,
            trim_ws = true,
            cellRange = CellRangeAddress.valueOf("E3:J10"),
            stopAtBlankLine = false,
            includeBlankLines = true
        )

        shouldContinueAtBlankDF.nrow shouldBe 7
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
    }

}