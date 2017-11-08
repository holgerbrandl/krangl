package krangl.test

import krangl.fromCSV
import org.junit.Test
import java.io.File

/**
 * @author Holger Brandl
 */

class IOTests {

    @Test
    fun testTornados() {
        val tornandoCsv = File("/Users/brandl/projects/kotlin/misc/tablesaw/data/1950-2014_torn.csv")

        val fromCSV = fromCSV(tornandoCsv)

    }
}