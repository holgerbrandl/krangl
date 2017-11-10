package krangl.test

import io.kotlintest.matchers.Matchers
import io.kotlintest.matchers.have
import krangl.*
import org.apache.commons.csv.CSVFormat
import org.junit.Test
import java.time.LocalDateTime


val irisData = DataFrame.fromCSV(DataFrame::class.java.getResourceAsStream("data/iris.txt"), format = CSVFormat.TDF.withHeader())
val flights = DataFrame.fromCSV(DataFrame::class.java.getResourceAsStream("data/nycflights.tsv.gz"), format = CSVFormat.TDF.withHeader(), isCompressed = true)


class SelectTest : Matchers {

    @Test
    fun `it should select with regex`() {
        sleepData.select { endsWith("wt") }.ncol shouldBe 2
        sleepData.select { endsWith("wt") }.ncol shouldBe 2
        sleepData.select { startsWith("sleep") }.ncol shouldBe 3
        sleepData.select { oneOf("conservation", "foobar", "order") }.ncol shouldBe 2

        sleepData.select<IntCol>()
        sleepData.select<StringCol>()

        sleepData.select2 { it is IntCol }
        sleepData.select2 { it.name.startsWith("foo") }

        val df = dataFrameOf("foo", "list_col", "date")(
            1, listOf(1,2,3), LocalDateTime.now()
        )

        df.select<LocalDateTime>()
    }


    @Test
    fun `it should allow to remove columns`() {
        // name,genus,vore,order,conservation,sleep_total,sleep_rem,sleep_cycle,awake,brainwt,bodywt

        sleepData.remove { endsWith("wt") }.ncol shouldBe 9
        sleepData.remove { startsWith("sleep") }.ncol shouldBe 9
        sleepData.remove { oneOf("conservation", "foobar", "order") }.ncol shouldBe 11

        sleepData.remove<IntCol>().ncol shouldBe 3
        irisData.remove<StringCol>().ncol shouldBe 4

        irisData.remove2 { it is StringCol }.ncol shouldBe 4
        irisData.remove2 { it.name.startsWith("Sepal") }.ncol shouldBe 3

        // also allow for negative selection (like in the context of gather)
        irisData.select { except { startsWith("Sepal") } }.ncol shouldBe 3
    }

    @Test
    fun `it should select non-existing column`() {
        try {
            sleepData.select("foobar")
            fail("foobar should not be selectable")
        } catch (t: Throwable) {
            // todo expect more descriptive exception here. eg. ColumnDoesNotExistException
        }
    }


    @Test
    fun `it should allow select no columns`() {
        try {
            sleepData.select(listOf())
            fail("should complain about mismatching selector array dimensionality")
        } catch (t: Throwable) {
        }

        sleepData.select(*arrayOf<String>()).ncol shouldBe 0
    }


    @Test
    fun `it should select same columns twice`() {
        // double selection is flattend out as in dplyr:  iris %>% select(Species, Species) %>% glimpse

        shouldThrow<IllegalArgumentException> {
            sleepData.select("name", "vore", "name").ncol shouldBe 2
        }

        sleepData.select("name", "vore").ncol shouldBe 2
    }


    @Test
    fun `it should do a negative selection`() {
        sleepData.remove("name", "vore").apply {
            names.contains("name") shouldBe false
            names.contains("vore") shouldBe false

            // ensure preserved order of remaining columns
            sleepData.names.minus(arrayOf("name", "vore")) shouldEqual names
        }

        irisData.select { !startsWith("Sepal") }.names shouldBe listOf("Petal.Length", "Petal.Width", "Species")
    }

    // krangl should prevent that negative and positive selections are combined in a single select() statement
    @Test
    fun `it should not allow a mixed negative and positive selection`() {
        // cf.  iris %>% select(ends_with("Length"), - Petal.Length) %>% glimpse()
        // not symmetric:  iris %>% select(- Petal.Length, ends_with("Length")) %>% glimpse()
        //  iris %>% select(-Petal.Length, ends_with("Length")) %>% glimpse()
        //        irisData.select({ endsWith("Length") }, { except("Petal.Length") }).apply {
        //            names shouldEqual listOf("Sepal.Length")
        //        }

        shouldThrow<InvalidColumnSelectException> {
            irisData.select { except("Species") AND startsWith("Sepal") }.structure().print()
        }

        //        irisData.select { except{startsWith("Sepal")} }.structure().print()
    }
}


class MutateTest : Matchers {

    @Test
    fun `rename columns and preserve their positions`() {
        sleepData.rename("vore" to "new_vore", "awake" to "awa2").apply {
            glimpse()
            names.contains("vore") shouldBe false
            names.contains("new_vore") shouldBe true

            // column renaming should preserve positions
            names.indexOf("new_vore") shouldEqual sleepData.names.indexOf("vore")

            // renaming should not affect column or row counts
            nrow == sleepData.nrow
            ncol == sleepData.ncol
        }
    }

    @Test
    fun `allow dummy rename`() {
        sleepData.rename("vore" to "vore").names shouldBe sleepData.names
    }

    @Test
    fun `it should  mutate existing columns while keeping their posi`() {
        irisData.addColumn("Sepal.Length" to { it["Sepal.Length"] + 10 }).names shouldBe irisData.names
    }

    @Test
    fun `it should  allow to use a new column in the same mutate call`() {
        sleepData.addColumns(
                "vore_new" to { it["vore"] },
                "vore_first_char" to { it["vore"].asStrings().ignoreNA { this.toList().first().toString() } }
        )
    }

    @Test
    fun `it should  allow add a rownumber column`() {
        sleepData.addColumn("user_id") {
            const("id") + rowNumber
        }["user_id"][1] shouldBe "id2"
    }

    @Test
    fun `it should gracefully reject incorrect type casts`() {
        shouldThrow<ColumnTypeCastException> {
            sleepData.addColumn("foo") { it["vore"].asInts() }
        }

    }
}


class FilterTest : Matchers {

    @Test
    fun `it should head tail and slic should extract data as expextd`() {
        // todo test that the right portions were extracted and not just size
        sleepData.head().nrow shouldBe 5
        sleepData.tail().nrow shouldBe 5
        sleepData.slice(1, 3, 5).nrow shouldBe 3
    }

    @Test
    fun `it should filter in empty table`() {
        sleepData
                .filter { it["name"] eq "foo" }
                // refilter on empty one
                .filter { it["name"] eq "bar" }
    }

    @Test
    fun `it should sub sample data`() {

        //        sleepData.count("vore").print()

        // fixed sampling should work
        sleepData.sampleN(2).nrow shouldBe 2
        sleepData.sampleN(1000, replace = true).nrow shouldBe 1000 // oversampling

        //  fractional sampling should work as well
        sleepData.sampleFrac(0.3).nrow shouldBe Math.round(sleepData.nrow * 0.3).toInt()
        sleepData.sampleFrac(0.3, replace = true).nrow shouldBe Math.round(sleepData.nrow * 0.3).toInt()
        sleepData.sampleFrac(2.0, replace = true).nrow shouldBe sleepData.nrow * 2 // oversampling

        // also test boundary conditions
        sleepData.sampleN(0).nrow shouldBe 0
        sleepData.sampleN(0, replace = true).nrow shouldBe 0
        sleepData.sampleFrac(0.0).nrow shouldBe 0
        sleepData.sampleFrac(0.0, replace = true).nrow shouldBe 0

        sleepData.sampleN(sleepData.nrow).nrow shouldBe sleepData.nrow
        sleepData.sampleN(sleepData.nrow, replace = true).nrow shouldBe sleepData.nrow
        sleepData.sampleFrac(1.0).nrow shouldBe sleepData.nrow
        sleepData.sampleFrac(1.0, replace = true).nrow shouldBe sleepData.nrow


        // make sure that invalid sampling parameters throw exceptions
        shouldThrow<IllegalArgumentException> { sleepData.sampleN(-1) }
        shouldThrow<IllegalArgumentException> { sleepData.sampleN(-1, replace = true) }
        shouldThrow<IllegalArgumentException> { sleepData.sampleFrac(-.3) }
        shouldThrow<IllegalArgumentException> { sleepData.sampleFrac(-.3, replace = true) }

        // oversampling without replacement should not work
        shouldThrow<IllegalArgumentException> { sleepData.sampleN(1000) }
        shouldThrow<IllegalArgumentException> { sleepData.sampleFrac(1.3) }


        // fixed sampling of grouped data should be done per group
        val groupCounts = sleepData.groupBy("vore").sampleN(2).count("vore")
        groupCounts["n"].asInts().distinct().apply {
            size shouldBe 1
            first() shouldBe 2
        }

        //  fractional sampling of grouped data should be done per group
        sleepData
                .groupBy("vore")
                .sampleFrac(0.5)
                .count("vore")
                .filter({ it["vore"] eq "omni" })
                .apply {
                    this["n"].asInts().first() shouldBe 10
                }
    }

}


class SortTest() : Matchers {

    @Test
    fun `use selector api for reverse sorting`() {
        val data = dataFrameOf("user_id")(
                2,
                3,
                4
        )

        data.sortedBy({ -it["user_id"] })["user_id"][0] shouldBe 4
    }
}


class SummarizeTest : Matchers {

    @Test
    fun `it should fail if summaries are not scalar values`() {
        shouldThrow<NonScalarValueException> {
            sleepData.summarize("foo", { listOf("a", "b", "c") })
        }
        shouldThrow<NonScalarValueException> {
            sleepData.summarize("foo", { BooleanArray(12) })
        }
    }

    class Something {
        override fun toString(): String = "Something(${hashCode()}"
    }

    @Test
    fun `it should should allow complex objects as summaries`() {


        sleepData.groupBy("vore").summarize("foo" to { Something() }, "bar" to { Something() }).print()
    }
}


class EmptyTest : Matchers {

    @Test
    fun `it should handle empty (row and column-empty) data-frames in all operations`() {
        SimpleDataFrame().apply {
            // structure
            ncol shouldBe 0
            nrow shouldBe 0
            rows.toList() should have size 0
            cols.toList() should have size 0

            // rendering
            glimpse()
            print()

            select(emptyList()) // will output warning
            // core verbs
            filter { BooleanArray(0) }
            addColumn("foo", { "bar" })
            summarize("foo" to { "bar" })
            sortedBy()

            // grouping
            (groupBy() as GroupedDataFrame).groups()
        }
    }
}


class GroupedDataTest : Matchers {

    /** dplyr considers NA as a group and krangl should do the same

    ```
    require(dplyr)

    iris
    iris$Species[1] <- NA

    ?group_by
    grpdIris <- group_by(iris, Species)
    grpdIris %>% slice(1)
    ```
     */
    @Test
    fun `it should allow for NA as a group value`() {

        // 1) test single attribute grouping with NA
        (sleepData.groupBy("vore") as GroupedDataFrame).groups().nrow shouldBe 5

        // 2) test multi-attribute grouping with NA in one or all attributes
        //        (sleepData.groupBy("vore") as GroupedDataFrame).groups().nrow shouldBe 6
        //todo implement me
    }


    @Test
    fun `it should count group sizes and report distinct rows in a table`() {
        // 1) test single attribute grouping with NA
        sleepData.count("vore").apply {
            print()
            ncol shouldBe 2
            nrow shouldBe 5
        }

        sleepData.distinct("vore", "order").apply {
            print()
            nrow shouldBe 32
            ncol shouldBe 11
        }
    }


    @Test
    fun `it should should auto-select grouping attributes from a grouped dataframe`() {
        //        flights.glimpse()
        val subFlights = flights
                .groupBy("year", "month", "day")
                //                .select({ range("year", "day") }, { oneOf("arr_delay", "dep_delay") })
                .select("arr_delay", "dep_delay", "year")

        subFlights.apply {
            ncol shouldBe 5
            (this is GroupedDataFrame) shouldBe true
            (this as GroupedDataFrame).groups.toList().first().df.ncol shouldBe 5
        }

    }


    @Test
    fun `it should calculate same group hash irrespective of column order`() {
        //        flights.glimpse()

        var dfA: DataFrame = dataFrameOf(
                "first_name", "last_name", "age", "weight")(
                "Max", "Doe", 23, 55,
                "Franz", "Smith", 23, 88,
                "Horst", "Keanes", 12, 82
        )

        val dfB = dfA.select("age", "last_name", "weight", "first_name")

        // by joining with multiple attributes we inherentily group (which is the actual test
        val dummyJoin = dfA.leftJoin(dfB, by = listOf("last_name", "first_name"))

        dummyJoin.apply {
            nrow shouldBe 3
        }
    }
}

