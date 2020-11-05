package krangl.test

import io.kotest.assertions.fail
import io.kotest.matchers.shouldBe
import krangl.*
import krangl.util.createValidIdentifier
import org.junit.Test
import java.lang.Math.abs
import java.util.*

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
            where(df["dep_delay"].toDoubles().mapNonNull { abs(it) > 3 }.nullAsFalse(), "red", "green")
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
                "A", 10, 0.1,
                "A", 50, 0.5,
                "A", 10, 0.1,
                "B", 100, 1.0,
                "B", 150, 1.5,
                "B", null, null,
                "B", 75, 0.75)

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
            pctChangeFor("B", "sales") shouldBe (listOf(null, 0.5, null, null))
            pctChangeFor("B", "price") shouldBe (listOf(null, 0.5, null, null))
        }
    }

}

class LeadLagTest{

    @Test
    fun `calculate lead and lag values`() {
        val sales = dataFrameOf("sales", "price")(
                10, 0.1,
                20, 0.2,
                null, null,
                40, 0.4,
                50, 0.5)

        val leadAndLag = sales
                .addColumn("sales_lead" to { it["sales"].lead() })
                .addColumn("price_lag" to { it["price"].lag(n = 2) })

        leadAndLag.apply {
            nrow shouldBe sales.nrow
            this["sales_lead"].values().asList() shouldBe (listOf(20, null, 40, 50, null))
            this["price_lag"].values().asList() shouldBe (listOf(null, null, 0.1, 0.2, null))
        }
    }

    @Test
    fun `lead lag column arithmetics`() {
        val sales = dataFrameOf("quarter", "sales", "store")(
                1, 30, "london",
                2, 10, "london",
                3, 50, "london",
                4, 10, "london",
                1, 100, "berlin",
                2, 150, "berlin",
                3, null, "berlin",
                4, 75, "berlin")


        sales.groupBy("store")
                .addColumn("quarter_diff" to { it["sales"] - it["sales"].lag(1) })
                .apply {
                    print()
                    nrow shouldBe sales.nrow

                    this["quarter_diff"][0] shouldBe null
                    this["quarter_diff"][1] shouldBe -20
                }

        sales.groupBy("store")
                .addColumn("lookahead_diff" to { it["sales"] - it["sales"].lead(1) }).apply {
                    print()
                    this["lookahead_diff"][0] shouldBe 20
                }

    }


    @Test
    fun `ensure custom defaults are added when using lead-lag`() {
        // test string
        irisData.addColumn("lagged" to {it["Species"].lead(1, "bla")}).apply {
            this["lagged"][nrow-1] shouldBe "bla"
        }

        // test numeric (with int default to add a bit complexity)
        irisData.addColumn("lagged"){ it["Sepal.Length"].lag(default = 42)}.let {
            it["lagged"][0] shouldBe 42.0
        }

        // test Any column
        val df = dataFrameOf("uuid")(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())

        //todo maybe it could more clever here to ensure type consistency in the column
        df.addColumn("prev_uuid" to { it["uuid"].lag(default = "foo") }).let {
            it["prev_uuid"][0] == "foo"
        }

        val defaultUid = UUID.randomUUID()
        df.addColumn("prev_uuid" to { it["uuid"].lag(default = defaultUid) }).let {
            it["prev_uuid"][0] == defaultUid
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


class MiscTests{

    // https://github.com/holgerbrandl/krangl/issues/98
    @Test
    fun `it should calculate the correct median`(){

        val numbers = arrayOf(1.0, 2.0, 3.0)
        val numbers2 = arrayOf(1.0, 2.0, 3.0, 4.0)
        val numbers3 = arrayOf(1.0)

        println("wrong")

        repeat(3) {
            val n = numbers.copyOf()
            n.shuffle()
            n.median() shouldBe 2.0
        }

        repeat(3) {
            val n = numbers2.copyOf()
            n.shuffle()
            n.median() shouldBe 2.5
        }

        repeat(3) {
            val n = numbers3.copyOf()
            n.shuffle()
            n.median() shouldBe 1.0
        }
    }
}
