package krangl.test;

import io.kotlintest.matchers.plusOrMinus
import io.kotlintest.matchers.shouldBe
import io.kotlintest.matchers.shouldEqual
import krangl.*
import org.junit.Test
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.net.URL
import java.util.*

/**
 * @author Holger Brandl
 */
class TypeInterfaceTest {


    val users = dataFrameOf(
        "firstName", "lastName", "age", "hasSudo")(
        "max", "smith", 53, false,
        "tom", "doe", 30, false,
        "eva", "miller", 23, true,
        null, "meyer", 23, null
    )

    data class User(val firstName: String?, val lastName: String, val age: Int, val hasSudo: Boolean?)

    @Test
    fun `it should preserve object columns across all core verbs`() {
        //TODO
    }


    @Test
    fun `it should allow to join on object columns`() {
        //TODO
    }


    @Test
    fun `it should convert data-classes to dataframes`() {
        data class Person(val name: String, val age: Int, val weight: Double)

        val users = listOf(
            Person("Anne", 23, 55.4),
            Person("Tina", 40, 60.4)
        )

        val df = users.asDataFrame()

        df.names shouldBe listOf("age", "name", "weight")
        df["age"][0] shouldBe 23
    }


    @Test
    fun `it should allow to map objects to df and back`() {
        val objPersons = users.rowsAs<User>()
        objPersons.toList().size shouldBe users.nrow


        // and back to df
        val df = objPersons.asDataFrame()
        df.nrow shouldBe 4
    }

    @Test
    fun `it should map rows to objects with custom mapping scheme`() {
        val objPersons = users
            .rename("firstName" to "Vorname")
            .rowsAs<User>(mapping = mapOf("Vorname" to "firstName"))

        objPersons.toList().size shouldBe users.nrow

        objPersons.first() shouldEqual User("max", "smith", 53, false)
    }


    @Test
    fun `it should provide the correct schema for object columns`() {
        val salaries = dataFrameOf("user", "salary")(User("Anna", "Doe", 23, null), 23.3)

        captureOutput {
            salaries.printDataClassSchema("Salary", receiverVarName = "salaries")
        }.first shouldBe """
        data class Salary(val user: Any, val salary: Double)
        val records = salaries.rowsAs<Salary>()
        """.trimIndent()

        data class Salary(val user: Any, val salary: Double)

        val records = salaries.rowsAs<Salary>()

        records.toList().size shouldBe 1
    }


    @Test
    fun `it should prevent illegal characters in generated schemas`() {
        captureOutput {
            irisData.printDataClassSchema(dataClassName = "IrisData", receiverVarName = "irisData")
        }.first.apply {
            print(this)
            contains("sepalLength") shouldBe true
            this shouldEqual """
            data class IrisData(val sepalLength: Double, val sepalWidth: Double, val petalLength: Double, val petalWidth: Double, val species: String)
            val records = irisData.rowsAs<IrisData>()
            """.trimIndent()
        }

        //use generated code to restore iris flowers
        data class IrisData(val sepalLength: Double, val sepalWidth: Double, val petalLength: Double, val petalWidth: Double, val species: String)

        val records = irisData.rowsAs<IrisData>()

        records.first().sepalLength shouldBe (5.1 plusOrMinus 0E-3)
    }


    /** prevent regressions from "Provide more elegant object bindings #22"*/
    @Test
    fun `it should print nullable data class schemes`() {
        val stdout = captureOutput { users.printDataClassSchema("User") }.first
        stdout shouldBe """
            data class User(val firstName: String?, val lastName: String, val age: Int, val hasSudo: Boolean?)
            val records = dataFrame.rowsAs<User>()
        """.trimIndent()
    }


    @Test
    fun `it should allow object columns`() {
        val id2 = irisData.groupBy("Species").summarize("url") { URL("https://github.com/holgerbrandl/krangl") }

        id2.ncol shouldBe (2)
        id2.print()
    }


    @Test
    fun `it should unwrap properties of object columns`() {
        val users = dataFrameOf("department", "ids")("tech", UUID.randomUUID(), "admin", UUID.randomUUID())
        val unwrapped = users.unfold<UUID>("ids", properties = listOf("variant"), keep = true)

        unwrapped["variant"][1] shouldEqual 2
    }

    data class Car(val ps: Int, val brand: String, val brandURL: URL) {
        val name: String = "${brand} ${ps}"

        fun alternativeURL() = brandURL

        fun alternativeURL(region: String) = brandURL
    }

    @Test
    fun `it should unwrap properties of data classes`() {

        val cars = dataFrameOf("cars")(
            Car(298, "Skoda", URL("http://www.skoda.de")),
            Car(200, "BMW", URL("http://www.bmw.de"))
        )

        cars["cars"].asType<Car>().first()!!.name

        val unfolded = cars.unfold<Car>("cars", keep = true)

        unfolded.names shouldBe listOf("cars", "brand", "brandURL", "name", "ps", "alternativeURL")
    }
}

internal fun captureOutput(expr: () -> Any): Pair<String, String> {
    val origOut = System.out
    val origErr = System.err
    // https://stackoverflow.com/questions/216894/get-an-outputstream-into-a-string

    val baosOut = ByteArrayOutputStream()
    val baosErr = ByteArrayOutputStream()

    System.setOut(PrintStream(baosOut));
    System.setErr(PrintStream(baosErr));


    // run the expression
    expr()

    val stdout = String(baosOut.toByteArray()).trim()
    val stderr = String(baosErr.toByteArray()).trim()

    System.setOut(origOut)
    System.setErr(origErr)

    return stdout to stderr
}
