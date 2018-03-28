package krangl.test

import io.kotlintest.matchers.shouldBe
import krangl.*
import org.apache.commons.csv.CSVFormat
import org.junit.Test
import java.io.File

/**
 * @author Holger Brandl
 */

class BuilderTests {

    @Test
    fun testTornados() {
        val tornandoCsv = File("src/test/resources/krangl/data/1950-2014_torn.csv")

        val df = DataFrame.readCSV(tornandoCsv)

        // todo continue test here
    }

    @Test
    fun `it should download and cache flights data locally`() {
        if (flightsCacheFile.exists()) flightsCacheFile.delete()
        (flightsData.nrow > 0) shouldBe true
    }

    enum class Engine { Otto, Other }
    data class Car(val name: String, val numCyl: Int?, val engine: Engine)

    @Test
    fun `it should convert objects into data-frames`() {

        val myCars = listOf(
            Car("Ford Mustang", null, Engine.Otto),
            Car("BMW Mustang", 3, Engine.Otto)
        )

        val carsDF = myCars.deparseRecords {
            mapOf(
                "model" to it.name,
                "motor" to it.engine,
                "cylinders" to it.numCyl)
        }

        carsDF.nrow shouldBe 2
        carsDF.names shouldBe listOf("model", "motor", "cylinders")

        // use enum order for sorting
        carsDF.columnTypes().print()

        //todo make sure that enum ordinality is used here for sorting
        carsDF.sortedBy { rowNumber }
        //        carsDF.sortedBy { it["motor"] }
        carsDF.sortedBy { it["motor"].asType<Engine>() }
        carsDF.sortedBy { it["motor"].map<Engine> { it.name } }
    }

    @Test
    fun `it should read a url`() {
        val df = DataFrame.readCSV("https://raw.githubusercontent.com/holgerbrandl/krangl/master/src/test/resources/krangl/data/1950-2014_torn.csv")
        assert(df.nrow > 2)
    }

    @Test
    fun `it should read and write compressed and uncompressed tables`() {
        //        createTempFile(prefix = "krangl_test", suffix = ".zip").let {
        //            sleepData.writeCSV(it)
        //            println("file was $it")
        //            DataFrame.readCSV(it).nrow shouldBe sleepData.nrow
        //        }

        createTempFile(prefix = "krangl_test", suffix = ".txt").let {
            sleepData.writeCSV(it, format = CSVFormat.TDF.withHeader())
            DataFrame.readCSV(it, format = CSVFormat.TDF.withHeader()).nrow shouldBe sleepData.nrow
        }
    }

    @Test
    fun `it should have the correct column types`() {

        val columnTypes = mapOf(
            "a" to ColType.String,
            "b" to ColType.String,
            "c" to ColType.Double,
            "e" to ColType.Boolean,
            ".default" to ColType.Int

        )

        val dataFrame = DataFrame.readCSV("src/test/resources/krangl/data/test_header_types.csv", colTypes = columnTypes)
        val cols = dataFrame.cols
        assert(cols[0] is StringCol)
        assert(cols[1] is StringCol)
        assert(cols[2] is DoubleCol)
        assert(cols[3] is IntCol)
    }

    val customNaDataFrame by lazy {
        DataFrame.readCSV("src/test/resources/krangl/data/custom_na_value.csv", format = CSVFormat.DEFAULT.withHeader().withNullString("CUSTOM_NA"))
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
    fun `it shoudl coerece lists and iterables to varargs when building inplace data-frames`() {
        dataFrameOf("foo")(listOf(1, 2, 3)).nrow shouldBe 3
        dataFrameOf("foo")(listOf(1, 2, 3).asIterable()).nrow shouldBe 3
        dataFrameOf("foo")(listOf(1, 2, 3).asSequence()).nrow shouldBe 3
    }


    @Test
    fun `it should enforce complete data when building inplace data-frames`() {

        // none
        shouldThrow<IllegalArgumentException> {
            dataFrameOf("user", "salary")()
        }

        // too few
        shouldThrow<IllegalArgumentException> {
            dataFrameOf("user", "salary")(1)
        }

        // too many
        shouldThrow<IllegalArgumentException> {
            dataFrameOf("user", "salary")(1, 2, 3)
        }

    }
}
