package krangl.test;

import io.kotest.matchers.doubles.plusOrMinus
import io.kotest.matchers.shouldBe
import krangl.*
import org.junit.Test
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.net.URL
import java.util.*

/**
 * @author Holger Brandl
 */
class TypeInterfaceTest {


    val users = dataFrameOf(
        "firstName", "lastName", "age", "hasSudo"
    )(
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


    // https://app.slack.com/client/T09229ZC6/C0AVAB92L/thread/C4W52CFEZ-1593092419.079100
    @Test
    fun `it should include parent class attributes in dataframes`() {
        open class Person(val name: String)
        class Manager(name: String, val isCEO: Boolean) : Person(name)

        val users = listOf(
            Manager("Anne", false),
            Manager("Tina", true)
        )

        val df = users.asDataFrame()
        df.cols.size shouldBe 2
        df.print()
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
    fun `it should allow to rename objet columns`() {
        dataFrameOf("id")(UUID.randomUUID()).rename("id" to "new_id").apply {
            names shouldBe listOf("new_id")
        }
    }

    @Test
    fun `it should map rows to objects with custom mapping scheme`() {
        val objPersons = users
            .rename("firstName" to "Vorname")
            .rowsAs<User>(mapping = mapOf("Vorname" to "firstName"))

        objPersons.toList().size shouldBe users.nrow

        objPersons.first() shouldBe User("max", "smith", 53, false)
    }


    @Test
    fun `it should provide the correct schema for object columns`() {
        val salaries = dataFrameOf("user", "salary")(User("Anna", "Doe", 23, null), 23.3)

        salaries.printDataClassSchema("Salary", receiverVarName = "salaries")

        captureOutput {
            salaries.printDataClassSchema("Salary", receiverVarName = "salaries")
        }.stdout shouldBe """
        data class Salary(val user: Any, val salary: Double)
        val records = salaries.rowsAs<Salary>()
        """.trimAndReline()

        data class Salary(val user: Any, val salary: Double)

        val records = salaries.rowsAs<Salary>()

        records.toList().size shouldBe 1
    }


    @Test
    fun `it should prevent illegal characters in generated schemas`() {
        captureOutput {
            irisData.printDataClassSchema(dataClassName = "IrisData", receiverVarName = "irisData")
        }.stdout.apply {
            print(this)
            contains("sepalLength") shouldBe true
            this shouldBe """
                    data class IrisData(val sepalLength: Double, val sepalWidth: Double, val petalLength: Double, val petalWidth: Double, val species: String)
                    val records = irisData.rowsAs<IrisData>()
                    """.trimAndReline()
        }

        //use generated code to restore iris flowers
        data class IrisData(
            val sepalLength: Double,
            val sepalWidth: Double,
            val petalLength: Double,
            val petalWidth: Double,
            val species: String
        )

        val records = irisData.rowsAs<IrisData>()

        records.first().sepalLength shouldBe (5.1 plusOrMinus 0E-3)
    }


    /** prevent regressions from "Provide more elegant object bindings #22"*/
    @Test
    fun `it should print nullable data class schemes`() {
        users.printDataClassSchema("User")
        val stdout = captureOutput { users.printDataClassSchema("User") }.stdout
        stdout shouldBe """
            data class User(val firstName: String?, val lastName: String, val age: Int, val hasSudo: Boolean?)
            val records = dataFrame.rowsAs<User>()
        """.trimAndReline()
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

        unwrapped["variant"][1] shouldBe 2
    }

    @Test

    fun `it should unwrap deeply nested properties of object columns`() {
        data class Applicant(val name: String, val bar: File, val stuff: List<URL>)

        val applicants = listOf(
            Applicant("max", File("cv.pdf"), listOf(URL("http://heise.de"), URL("http://github.com"))),
            Applicant("moritz", File("docs.pdf"), listOf(URL("http://bintray.com")))
        )
        val applDF = applicants.asDataFrame()

        applDF.print()
        //        applDF.schema()

        // unfold is wrong here because we don't want to unfold (horizontally) but vertically
        //        val unwrapped = users.unfold<UUID>("ids", properties = listOf("variant"), keep = true)


        val flatDF = applDF.unnest("stuff")

        flatDF.print()
        flatDF.nrow shouldBe 3
        flatDF.names shouldBe listOf("bar", "name", "stuff")
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

        val unfoldedWithPrefix = cars.unfold<Car>("cars", keep = false, addPrefix = true)
        unfoldedWithPrefix.names shouldBe listOf(
            "cars_brand",
            "cars_brandURL",
            "cars_name",
            "cars_ps",
            "cars_alternativeURL"
        )
    }


}

class ValueClassTests {
    data class Something(
        val something: SomeValue,
        val somethingNullable: SomeValue?,
        val foo: String,
        val bar: Int?,
        val category: SomeEnum
    )

    enum class SomeEnum {
        NORMAL, OTHER,
    }


    @JvmInline
    value class SomeValue(val id: String) {
        override fun toString(): String = id
    }


    @Test
    fun `it should support nullable value class properties in asDataFrame`() {
        val df = listOf(
            Something(SomeValue("123"), null, "hey", 42, SomeEnum.NORMAL),
            Something(SomeValue("456"), SomeValue("456"), "ho", null, SomeEnum.NORMAL)
        ).asDataFrame()

        df.nrow shouldBe 2
    }
}

internal data class CapturedOutput(val stdout: String, val stderr: String)

internal fun captureOutput(expr: () -> Any): CapturedOutput {
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

    return CapturedOutput(stdout, stderr)
}

// since the test reference dat is typically provided as multi-line which is using \n by design, we adjust the
// out-err stream results accordingly here to become platform independent.
// https://stackoverflow.com/questions/48933641/kotlin-add-carriage-return-into-multiline-string
internal fun String.trimAndReline() = trimIndent().replace("\n", System.getProperty("line.separator"))

