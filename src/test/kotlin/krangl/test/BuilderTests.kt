package krangl.test

import io.kotlintest.shouldBe
import krangl.*
import org.apache.commons.csv.CSVFormat
import org.junit.Test
import java.io.File
import java.io.FileReader

/**
 * @author Holger Brandl
 */

class CsvReaderTests {

    @Test
    fun `skip lines and read file without header`() {
        val dataFile = File("src/test/resources/krangl/data/headerless_with_preamble.txt")
        val predictions = DataFrame.readDelim(FileReader(dataFile), skip = 7, format = CSVFormat.TDF)

        predictions.apply {
            nrow shouldBe 13
            names shouldBe listOf("X1", "X2", "X3")
            head().print()
        }
    }


    @Test
    fun testTornados() {
        val tornandoCsv = File("src/test/resources/krangl/data/1950-2014_torn.csv")

        val df = DataFrame.readCSV(tornandoCsv)

        // todo continue test here
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
                "f" to ColType.Long,
                ".default" to ColType.Int

        )

        val dataFrame = DataFrame.readCSV("src/test/resources/krangl/data/test_header_types.csv", colTypes = columnTypes)
        val cols = dataFrame.cols
        assert(cols[0] is StringCol)
        assert(cols[1] is StringCol)
        assert(cols[2] is DoubleCol)
        assert(cols[3] is IntCol)
        assert(cols[4] is BooleanCol)
        assert(cols[5] is LongCol)
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

}

class BuilderTests {

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
        columnTypes(carsDF).print()

        //todo make sure that enum ordinality is used here for sorting
        carsDF.sortedBy { rowNumber }
        //        carsDF.sortedBy { it["motor"] }
        carsDF.sortedBy { it["motor"].asType<Engine>() }
        carsDF.sortedBy { it["motor"].map<Engine> { it.name } }
    }

    @Test
    fun `it should convert object with extractor patterns`() {
        sleepPatterns.deparseRecords(
                "foo" with { awake },
                "bar" with { it.brainwt?.plus(3) }
        ).apply {
            print()
            schema()
            names shouldBe listOf("foo", "bar")
        }
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


    @Test
    fun `it should convert row records in a data frame`() {
        val records = listOf(
                mapOf("name" to "Max", "age" to 23),
                mapOf("name" to "Anna", "age" to 42)
        )

        dataFrameOf(records).apply {
            print()
            nrow shouldBe 2
            names shouldBe listOf("name", "age")
        }
    }
}


class JsonTests {


    @Test
    fun `it should read json data from url`() {
        val df = DataFrame.fromJson("https://raw.githubusercontent.com/vega/vega/master/docs/data/movies.json")

        df.apply {
            nrow shouldBe 3201
            names.last() shouldBe "IMDB Votes"
        }
    }

    @Test
    fun `it should read json data from json string`() {
        val df = DataFrame.fromJsonString("""
            {
                "cars": {
                    "Nissan": [
                        {"model":"Sentra", "doors":4},
                        {"model":"Maxima", "doors":4},
                        {"model":"Skyline", "doors":2}
                    ],
                    "Ford": [
                        {"model":"Taurus", "doors":4},
                        {"model":"Escort", "doors":4, "seats":5}
                    ]
                }
            }
            """)

        df.apply {
            schema()
            print()
            nrow shouldBe 5
            names shouldBe listOf("_id", "cars", "model", "doors", "seats")
        }
    }


    @Test
    fun `it should read incomplete json data from json string`() {
        val df = DataFrame.fromJsonString("""
            {
               "Nissan": [
                        {"model":"Sentra", "doors":4},
                        {"model":"Maxima", "doors":4},
                        {"model":"Skyline", "seats":9}
                    ],
            }
            """)

        df.apply {
            schema()
            print()
            nrow shouldBe 3
            names shouldBe listOf("_id", "model", "doors", "seats")
        }
    }


    @Test
    fun `it should convert numerical data-frames to matrices, but should fail for mixed type dfs`() {

        shouldThrow<IllegalArgumentException> { irisData.toDoubleMatrix() }
        shouldThrow<IllegalArgumentException> { irisData.toFloatMatrix() }

        irisData.remove("Species").toDoubleMatrix().apply {
            size shouldBe 4
            first().size shouldBe irisData.nrow
        }
    }
}

class ExcelTests {

    @Test
    fun `it should read excel file`(){
        val headerHigherThanContentDF = DataFrame.readExcel("src/test/resources/krangl/data/ExcelReadExample.xlsx",
                "FirstSheet",102 )

        headerHigherThanContentDF shouldBe emptyDataFrame()

        //Test Sheet by name starting at row 1
        val df = DataFrame.readExcel("src/test/resources/krangl/data/ExcelReadExample.xlsx",
                "FirstSheet",1 )

        df.names shouldBe listOf("Name", "Email", "BirthDate", "Country")
        df.nrow shouldBe 100
        df["Name"][0] shouldBe "Hyatt"
        df.print("ExcelReadTest")

        // Test sheet by index + cell range
        val rowSkipTestDF = DataFrame.readExcel("src/test/resources/krangl/data/ExcelReadExample.xlsx",
                1,3 )

        rowSkipTestDF shouldBe df

    }
    @Test
    fun `it should write to excel`(){
        val df = DataFrame.readExcel("src/test/resources/krangl/data/ExcelReadExample.xlsx",
                "FirstSheet",1 )

        df.writeExcel("src/test/resources/krangl/data/ExcelWriteResult.xlsx", "FirstSheet", true, true, false)
        df.writeExcel("src/test/resources/krangl/data/ExcelWriteResult.xlsx", "SecondSheet", true, false, true)
        df.writeExcel("src/test/resources/krangl/data/ExcelWriteResult.xlsx", "ThirdSheet", false,false, false)

        val writtenDF = DataFrame.readExcel("src/test/resources/krangl/data/ExcelWriteResult.xlsx",
                "FirstSheet",1 )

        writtenDF shouldBe df
    }

}
