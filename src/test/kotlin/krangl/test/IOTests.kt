package krangl.test

import io.kotlintest.matchers.Matchers
import krangl.*
import org.junit.Test
import java.io.File

/**
 * @author Holger Brandl
 */

class IOTests : Matchers {

    @Test
    fun testTornados() {
        val tornandoCsv = File("src/test/resources/krangl/data/1950-2014_torn.csv")

        val fromCSV = DataFrame.fromCSV(tornandoCsv)

        // todo continue test here
    }

    @Test fun useCachedFiles(){
        flightsCacheFile.delete()
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

        val carsDF = myCars.asDataFrame { mapOf(
                "model" to it.name,
                "motor" to it.engine,
                "cylinders" to it.numCyl)
        }

        carsDF.nrow shouldBe 2
        carsDF.names shouldBe listOf("model", "motor", "cylinders")

        // use enum order for sorting
        carsDF.structure().print()

        carsDF.sortedBy { it["motor"].asType<Engine>() }
    }
}