package krangl.test

import io.kotest.matchers.shouldBe
import krangl.dataFrameOf
import krangl.eq
import krangl.print
import org.junit.Test

class RowTests {

    @Test
    fun `it should add a new row`() {
        var df = (dataFrameOf(
            "Col1", "Col2"
        ))(
            "Row1", 1,
            "Row2", 2,
            "Row3", 3
        )

        df = df.addRow(listOf("Row4", 4))

        //No columns should be added/removed
        df.ncol shouldBe 2

        //Number of rows should be one more than the initial number
        df.nrow shouldBe 4

        // Col1 of dataframe should have the previous values plus the new one
        df["Col1"].values() shouldBe arrayOf("Row1", "Row2", "Row3", "Row4")

        // Col2 of dataframe should have the previous values plus the new one
        df["Col2"].values() shouldBe arrayOf(1, 2, 3, 4)

        // Col2 of the new row should contain the new value
        df.filter { it["Col1"] eq "Row4" }["Col2"].values() shouldBe arrayOf(4)
        df.print()
    }

}