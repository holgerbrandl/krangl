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


    @Test
    fun `calculate cummulative sum in grouped dataframe including NAs`() {
        val sales = dataFrameOf("product", "sales")(
                "A", 32.3,
                "A", 12.2,
                "A", 24.2,
                "B", 23.3,
                "B", 12.3,
                "B", null,
                "B", 2.5)

//        sales.summarize("mean_sales" to { it["sales"].mean(removeNA = true)})
//        sales.addColumn("cum_sales" to { it["sales"].cumSum()}).print()

        val cumSumGrd = sales.groupBy("product").addColumn("cum_sales" to { it["sales"].cumSum() })

        cumSumGrd.apply {
            print()
            nrow shouldBe sales.nrow
            this["cum_sales"][1] shouldBe 44.5
            this["cum_sales"][4] shouldBe 35.6
            this["cum_sales"][5] shouldBe null
            this["cum_sales"][6] shouldBe null
        }
    }

    @Test
    fun `calculate percentage change in grouped dataframe including NAs`() {
        val sales = dataFrameOf("product", "sales", "price")(
                "A", null, null,
                "A", 10,   0.1,
                "A", 50,   0.5,
                "A", 10,   0.1,
                "B", 100,  1.0,
                "B", 150,  1.5,
                "B", null, null,
                "B", 75,   0.75)

        val pctChangeGrd = sales.groupBy("product")
                .addColumn("sales_pct_change" to { it["sales"].pctChange() })
                .addColumn("price_pct_change" to { it["price"].pctChange() })

        pctChangeGrd.apply {
            fun pctChangeFor(product: String, col: String) =
                    filter { it["product"] eq product }[col + "_pct_change"].values().asList()

            print()
            nrow shouldBe sales.nrow
            pctChangeFor("A", "sales") shouldBe (listOf(null, null, 4.0, -0.8))
            pctChangeFor("A", "price") shouldBe (listOf(null, null, 4.0, -0.8))
            pctChangeFor("B", "sales") shouldBe (listOf(null, 0.5, 0.0, -0.5))
            pctChangeFor("B", "price") shouldBe (listOf(null, 0.5, 0.0, -0.5))
        }
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
