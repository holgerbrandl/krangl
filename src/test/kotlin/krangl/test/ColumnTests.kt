package krangl.test

import io.kotlintest.fail
import io.kotlintest.shouldBe
import krangl.*
import krangl.util.createValidIdentifier
import org.junit.Test
import java.lang.Math.abs

/**
 * @author Holger Brandl
 */
class ColumnTests {

    @Test
    fun `it should do correct column arithmetics`() {

        (IntCol("", listOf(3)) + 3)[0] shouldBe 6
        (IntCol("", listOf(3)) + 3.0)[0] shouldBe 6.0
        (IntCol("", listOf(3)) + "foo")[0] shouldBe "3foo"
    }

    @Test
    fun `it should do correct string column arithmetics`() {

        irisData.addColumns(
            // TODO this does not get a compiler warning, but should in this context
            //            "initials" to { it["Species"].map<String> { it.first() } + it["Species"].map<String> { it.first() } },
            "initials" to { it["Species"].map<String> { it.first() } concat it["Species"].map<String> { it.first() } }
        )
    }


    @Test
    fun `allow to negate and invert columns`() {

        (!BooleanCol("foo", listOf(false, true)))[0] shouldBe true

        (-IntCol("foo", listOf(1, 2)))[1] shouldBe -2
        (-LongCol("foo", listOf(1L, 2L)))[1] shouldBe -2L
        (-DoubleCol("foo", listOf(1.2, 2.0)))[1] shouldBe -2.0


        shouldThrow<UnsupportedOperationException> { (-BooleanCol("foo", listOf(true))) }
        shouldThrow<UnsupportedOperationException> { (!IntCol("foo", listOf(1))) }
        shouldThrow<UnsupportedOperationException> { (!LongCol("foo", listOf(1L))) }

        //
        shouldThrow<UnsupportedOperationException> { (!AnyCol("foo", listOf(1))) }
        shouldThrow<UnsupportedOperationException> { (-AnyCol("foo", listOf(1))) }

    }


    // https://github.com/holgerbrandl/krangl/issues/54
    @Test
    fun `allow to create new column conditionally`() {
        //        irisData.addColumn("trimmed_petal_length") {
        //            where(it["Petal.Length"] gt 1.5, 1.5, it["Petal.Length"])
        //        }.print()

        //        // bad example: trimming could be done with just
        //        irisData.addColumn("trimmed_petal_length"){ df ->
        //            df[PETAL_LENGTH].map<Double>{ Math.max(it, 1.3)}
        //        }

        // or using basic mapping
        //        irisData.addColumn("foo"){ df -> (df["Sepal.Length"] gt 1.3).map{ if(it) 1.3 else df["Petal.Length"].asDoubles() } }.schema()

        flightsData.addColumn("delay_category") { df ->
            where(df["dep_delay"].asDoubles().mapNonNull { abs(it) > 3 }.nullAsFalse(), "red", "green")
        }
    }

    @Test
    fun `wrap column name with backticks if necessary`() {
        val regularColumn = BooleanCol("simple_column", listOf(true, false))
        val spaceColumn = BooleanCol("space column", listOf(true, false))

        createValidIdentifier(regularColumn.name) shouldBe "simple_column"
        createValidIdentifier(spaceColumn.name) shouldBe "spaceColumn"
    }

    @Test
    fun `compare columns correctly`() {
        // a int, b double
        val df = dataFrameOf("a", "b")(1, 1.5, 3, 2.5, 4, 4.0)

        (df.addColumn("foo") { it["a"] gt it["b"] }["foo"].values() contentEquals arrayOf<Boolean?>(false, true, false)) shouldBe true
        (df.addColumn("foo") { it["a"] ge it["b"] }["foo"].values() contentEquals arrayOf<Boolean?>(false, true, true)) shouldBe true
    }
}


internal inline fun <reified T> shouldThrow(thunk: () -> Any): T {
    val e = try {
        thunk()
        null
    } catch (e: Exception) {
        e
    }

    if (e == null)
        fail("Expected exception ${T::class.qualifiedName} but no exception was thrown")
    else if (e.javaClass.name != T::class.qualifiedName) {
        e.printStackTrace()
        fail("Expected exception ${T::class.qualifiedName} but ${e.javaClass.name} was thrown")
    } else
        return e as T
}
