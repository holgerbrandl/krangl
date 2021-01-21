package krangl.test

import io.kotest.assertions.fail
import io.kotest.matchers.collections.haveSize
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import krangl.*
import org.apache.commons.csv.CSVFormat
import org.junit.Assert.assertEquals
import org.junit.Test
import java.util.*


val flights = DataFrame.readDelim(DataFrame::class.java.getResourceAsStream("data/nycflights.tsv.gz"), format = CSVFormat.TDF.withHeader(), isCompressed = true)


class SelectTest {

    @Test
    fun `allow for empty data frame`() {
        emptyDataFrame().print()
        columnTypes(emptyDataFrame())
        emptyDataFrame().head()
        emptyDataFrame().tail()
        emptyDataFrame().select<IntCol>()

    }

    @Test
    fun `it should select with regex`() {
        sleepData.select { endsWith("wt") }.ncol shouldBe 2
        sleepData.select { endsWith("wt") }.ncol shouldBe 2
        sleepData.select { startsWith("sleep") }.ncol shouldBe 3
        sleepData.select { listOf("conservation", "foobar", "order") }.ncol shouldBe 2

        sleepData.select<IntCol>()

        sleepData.selectIf { it is IntCol }
        sleepData.selectIf { it.name.startsWith("foo") }


        // type based select
        irisData.select<StringCol>().names shouldBe listOf("Species")


        // can we also filter by type in object columns
        //        val df = dataFrameOf("foo", "list_col", "date")(
        //                1, listOf(1,2,3), LocalDateTime.now()
        //        )
        //        df.select<LocalDateTime>().names shouldBe listOf("date")
    }


    @Test
    fun `it should allow to remove columns`() {
        // name,genus,vore,order,conservation,sleep_total,sleep_rem,sleep_cycle,awake,brainwt,bodywt

        sleepData.remove { endsWith("wt") }.ncol shouldBe 9
        sleepData.remove { startsWith("sleep") }.ncol shouldBe 8
        sleepData.remove { listOf("conservation", "foobar", "order") }.ncol shouldBe 9

        dataFrameOf("foo", "bar")(1, "huhu").remove<IntCol>().names shouldBe listOf("bar")
        irisData.remove<StringCol>().ncol shouldBe 4

        // disabled we wanted to constrain T to DataCol in remove/select<T>
        // dataFrameOf("foo", "bar")(1, LocalDateTime.now()).remove<LocalDateTime>().names shouldBe listOf("bar")


        irisData.removeIf { it is StringCol }.ncol shouldBe 4
        irisData.removeIf { it.name.startsWith("Sepal") }.ncol shouldBe 3

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

        @Suppress("RemoveRedundantSpreadOperator")
        sleepData.select(*arrayOf<String>()).ncol shouldBe 0

        irisData.select { startsWith("bla") }.ncol shouldBe 0

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
            sleepData.names.minus(arrayOf("name", "vore")) shouldBe names
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


        // note: typically the user would perform a positive selection but in context like gather he needs a negative selection api as well
        columnTypes(irisData.select { except("Species") AND !startsWith("Sepal") }).print()
        columnTypes(irisData.select { except("Species") AND except { startsWith("Sepal") } }).print()

        // but she must never mix positive and negative selection
        shouldThrow<InvalidColumnSelectException> {
            columnTypes(irisData.select { except("Species") AND startsWith("Sepal") }).print()
        }

        //        irisData.select { except{startsWith("Sepal")} }.structure().print()
    }

    @Test
    fun `it should handle empty negative selections gracefully`() {
        //        irisData.select { !startsWith("Species")}.print()
        irisData.select { except() }.print()

    }

    @Test
    fun `it should allow to select with matchers in grouped df`() {
        irisData.groupBy("Species")
                .select { endsWith("Length") }.apply {
                    print()
                    names shouldBe listOf("Species", "Sepal.Length", "Petal.Length")
                }

    }
}


class AddColumnTest {

    @Test
    fun `rename columns and preserve their positions`() {
        sleepData.rename("vore" to "new_vore", "awake" to "awa2").apply {
            schema()
            names.contains("vore") shouldBe false
            names.contains("new_vore") shouldBe true

            // column renaming should preserve positions
            names.indexOf("new_vore") shouldBe sleepData.names.indexOf("vore")

            // renaming should not affect column or row counts
            nrow shouldBe sleepData.nrow
            ncol shouldBe sleepData.ncol
        }
    }

    @Test
    fun `allow dummy rename`() {
        sleepData.rename("vore" to "vore").names shouldBe sleepData.names
    }

    @Test
    fun `it should  mutate existing columns while keeping their position`() {
        irisData.addColumn("Sepal.Length" to { it["Sepal.Length"] + 10 }).names shouldBe irisData.names
    }

    @Test
    fun `it should  allow to use a new column in the same mutate call`() {
        sleepData.addColumns(
                "vore_new" to { it["vore"] },
                // old API
                // "vore_first_char" to { it["vore"].asStrings().mapNonNull { it.toList().first().toString() } }
                // more modern
                "vore_first_char" to { it["vore"].map<String> { it.toList().first().toString() } }
        )
    }

    @Test
    fun `it should  allow add a rownumber column`() {
        sleepData.addColumn("user_id") {
            const("id") + rowNumber
        }["user_id"][1] shouldBe "id2"

        // again but with explicit type convertion
        sleepData.addColumn("user_id") {
            const("id").asType<String>().zip(rowNumber).map { (l, r) -> l!! + r }
        }["user_id"][1] shouldBe "id2"


        sleepData.addRowNumber().names.first() shouldBe "row_number"
    }


    @Test
    fun `it should gracefully reject incorrect type casts`() {
        shouldThrow<NumberFormatException> {
            sleepData.addColumn("foo") { it["vore"].toInts() }
        }

    }

    @Test
    fun `it should allow to create columns from Any scalars`() {
        val someObject = "foo".toRegex()

        dataFrameOf("foo")("bar")
                .addColumn("some_any") { someObject }.apply {
                    names shouldBe listOf("foo", "some_any")
                    this[1][0] shouldBe someObject
                }
    }


    @Test
    fun `it should perform correct column arithmetics`() {
        val data = dataFrameOf("product", "weight", "price", "num_items", "inflammable")(
                "handy", 2.0, 1.0, 33, true,
                "tablet", 1.5, 6.0, 22, true,
                "macbook", 12.5, 20.0, 4, false
        )

        data.addColumn("price_per_kg") { it["price"] / it["weight"] }["price_per_kg"].toDoubles() shouldBe
                arrayOf<Double?>(0.5, 4.0, 1.6)

        data.addColumn("value") { it["num_items"] * it["price"] }["value"].toDoubles() shouldBe
                arrayOf<Double?>(33.0, 132.0, 80.0)


        // same but with reversed arguments
        data.addColumn("value") { it["price"] * it["num_items"] }["value"].toDoubles() shouldBe
                arrayOf<Double?>(33.0, 132.0, 80.0)
    }


    @Test
    fun `it should do implicit column type casts`() {
        dataFrameOf("foo")(1, 2, 3).addColumn("stringified_foo") { it["foo"].toStrings() }.schema()
        dataFrameOf("foo")("1", "2", "3").addColumn("parsed_foo") { it["foo"].toInts() }.schema()
    }

}


class FilterTest {

    @Test
    fun `it should head tail and slic should extract data as expextd`() {
        // todo test that the right portions were extracted and not just size
        sleepData.take().nrow shouldBe 5
        sleepData.takeLast(5).nrow shouldBe 5
        sleepData.slice(1, 3, 5).nrow shouldBe 3
        sleepData.slice(3..5).nrow shouldBe 3
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
        groupCounts["n"].toInts().distinct().apply {
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
                    this["n"].toInts().first() shouldBe 10
                }
    }

    @Test
    fun `it should filter rows with text matching helpers`() {
        sleepData.filter { it["vore"].isMatching<String> { equals("insecti") } }.nrow shouldBe 5
        sleepData.filter { it["vore"].isMatching<String> { startsWith("ins") } }.nrow shouldBe 5


        val df = dataFrameOf("x")(1, 2, 3, 4, 5, null)
        df.filter { it["x"] gt 2 }.apply {
            filter { isNA("x") }.nrow shouldBe 0
            nrow shouldBe 3
        }
        df.filter { it["x"] ge 2 }.nrow shouldBe 4
        df.filter { it["x"] lt 2.0 }.nrow shouldBe 1
        df.filter { it["x"] le 2f }.nrow shouldBe 2
    }

    @Test
    fun `it should infer boolean-list type in filter extensions`() {
        //fixme; could be a compiler bug
        // see https://medium.com/@quiro91/getting-to-know-kotlins-extension-functions-some-caveats-to-keep-in-mind-d14d734d108b
        //        persons.filter { it["last_name"].asStrings().map { it!!.startsWith("Do") } }
        //        // this would work
        //        persons.filter { it["last_name"].asStrings().map { it!!.startsWith("Do") }.toBooleanArray() }
    }


    @Test
    fun `it should allow for vectorized filter expressions`() {
        irisData.filter { (it["Sepal.Length"] gt it["Petal.Length"] * 3) AND (it["Species"] eq "setosa") }.apply {
            print()
            nrow shouldBe 44
        }
    }

    @Test
    fun `it should support filtering by list`() {
        irisData.filter { it["Species"].inList("setosa", "versicolor") }.apply {
            print()
            nrow shouldBe 100
        }
    }

}

fun <T> DataCol.inList(vararg filters: T): BooleanArray {
    val filtersList = listOf(*filters)

    return values().map { filtersList.contains(it) }.toBooleanArray()
}


class SortTest() {

    val data = dataFrameOf("user_id", "name")(
            6, "maja",
            3, "anna",
            null, "max",
            5, null,
            1, "tom",
            5, "tom"
    )

    @Test
    fun `it should implement order and rank the same as r`() {

        /*

        # rank returns the order of each element in an ascending list
        # order returns the index each element would have in an ascending list

        y = c(3.5, 3, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1 )
        rank(y, ties="first")-1
        order(y)-1 # thats our "rank"
         */

        val y = DoubleCol("foo", listOf(3.5, 3.0, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1))

        y.rank() shouldBe listOf(7, 1, 4, 2, 8, 9, 5, 6, 0, 3)
        y.order() shouldBe listOf(8, 1, 3, 9, 2, 6, 7, 0, 4, 5)
    }


    @Test
    fun `sort numeric columns`() {

        data.sortedBy("user_id").also {
            it.print()
        }["user_id"]
                .run {
                    print(values().asList())
                    get(0) shouldBe 1
                    toInts() shouldBe arrayOf<Int?>(1, 3, 5, 5, 6, null)
                }

        data.sortedBy { it["user_id"] }["user_id"][0] shouldBe 1
        data.sortedBy { -it["user_id"] }["user_id"][0] shouldBe 6
    }


    @Test
    fun `sort numeric columns descending`() {
        data.sortedByDescending("user_id").also { println(it) }.also {
            it["user_id"][0] shouldBe 6
            it["name"][0] shouldBe "maja"
            it["user_id"][5] shouldBe null
            it["name"][5] shouldBe "max"
        }


        // also check sorting order if NA's a are present in data (they should come last)
        sleepData.sortedByDescending("sleep_rem").run {
            //            print(maxRows = -1)
            this["sleep_rem"][0] shouldBe 6.6
        }
    }

    @Test
    fun `resolve ties if needed`() {
        // the test would require a tie-resolve if sleep_rem would be included as second sorting attribute
        sleepData
                //            .filter { it["order"].isEqualTo("Artiodactyla") }
                //            .also { print(it) }
                .sortedBy("order", "sleep_total").run {
                    get("sleep_total").toDoubles()[1] shouldBe 1.9
                }

        // also mix asc and desc sorting
        sleepData
                .sortedBy({ it["order"] }, { desc("sleep_total") }).run {
                    get("sleep_total").toDoubles()[1] shouldBe 9.1 // most sleep one among Artiodactyla
                }

    }

    @Test
    fun `it should sort text columns asc and desc`() {
        //asc
        data.sortedBy { it["name"] }["name"][0] shouldBe "anna"
        data.sortedBy("name")["name"][0] shouldBe "anna"

        //desc
        data.sortedBy { desc(it["name"]) }["name"][0] shouldBe "tom"
        data.sortedByDescending("name")["name"][0] shouldBe "tom"
    }


    @Test
    // todo the better design would be to add more type constraints to `SortExpression`
    fun `it should fail for invalid sorting predicates`() {
        shouldThrow<InvalidSortingPredicateException> {
            sleepData.sortedBy { "order" }.print()
        }
    }


}


class SummarizeTest {

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

    @Test
    fun `count should behave like dplyr-count`() {
        irisData.count() shouldBe dataFrameOf("n")(150)

        // prevent duplicated column names
        shouldThrow<DuplicatedColumnNameException> {
            irisData.count().count("n")
        }.message shouldBe "'n' is already present in data-frame"

        irisData.count().count("n", name = "new_n").names shouldBe listOf("n", "new_n")

        // is an existing grouping preserved
        irisData.groupBy("Species").count().nrow shouldBe 3


    }

    @Test
    fun `count should work with function literals`() {
        sleepData.addColumns("sleep_na" to { it["sleep_rem"].isNA() }).count("sleep_na").print()


        // should be equivalent to
        sleepData.groupByExpr { it["sleep_rem"].isNA() }.count().print()
        sleepData.groupByExpr({ it["sleep_rem"].isNA() }, { it["sleep_rem"].isNA() }).count().print()
        sleepData.groupByExpr().count().print()

        sleepData.countExpr { it["sleep_rem"].isNA() }.print()

    }

    @Test
    fun `summarize multiple columns at once with summarizeEach`() {
        // using builder style api
        irisData.summarizeAt({ startsWith("Sepal") }) {
            add({ mean() }, "mean")
            add({ median() }, "median")
        }.apply {
            print()
            nrow shouldBe 1
            names.size shouldBe 4
        }


        // using varargs
        irisData.summarizeAt({ endsWith("Length") },
                SumFuns.mean,
                //            AggFun({ mean() }),
                AggFun({ median() }, "median")
        ).apply {
            print()
            nrow shouldBe 1
            names.size shouldBe 4
        }
    }

    @Test
    fun `it should allow to drop cells with missing data across multiple columns`() {
        // see https://github.com/holgerbrandl/krangl/issues/105

        val df = dataFrameOf("user_id", "name", "age")(
                6, "maja", 23,
                3, "anna", 44,
                null, "max", 22,
                5, null, 56,
                1, "tom", null,
                5, "tom", 19
        )

        val expected = dataFrameOf("user_id", "name", "age")(
                6, "maja", 23,
                3, "anna", 44,
                5, "tom", 19
        )

        df.filterByRow { !it.values.contains(null)
        } shouldBe expected

        df.filterNotNull() shouldBe expected
        df.filterNotNull { startsWith("user") } shouldBe
                df.filter{ rowNumber.map{ listOf(1,2,4,5,6).contains(it)}.toBooleanArray() }
    }


    @Test
    fun `summarize multiple columns in grouped data frames with summarizeEach`() {

        irisData.groupBy("Species")
                .summarizeAt({ endsWith("Length") }, SumFuns.mean).apply {
                    print()
                    nrow shouldBe 3
                    names shouldBe listOf("Species", "Sepal.Length.mean", "Petal.Length.mean")
                }

    }
}


class CoreTests {

    @Test
    fun `it should handle empty (row and column-empty) data-frames in all operations`() {
        SimpleDataFrame().apply {
            // structure
            ncol shouldBe 0
            nrow shouldBe 0
            rows.toList() should haveSize(0)
            cols.toList() should haveSize(0)

            // rendering
            schema()
            print()

            select(emptyList()) // will output warning
            // core verbs
            filter { BooleanArray(0) }
            addColumn("foo", { "bar" })
            summarize("foo" to { "bar" })
            sortedBy()

            // grouping
            (groupBy() as GroupedDataFrame).groupedBy()
        }
    }


    @Test
    fun `it should round numbers when printing`() {
        val df = dataFrameOf("a")(Random().apply { setSeed(3) }.nextDouble(), null)

        df.asString(maxDigits = 5) shouldBe """
            A DataFrame: 2 x 1
                      a
            1   0.73106
            2      <NA>
            """.trimAndReline()
    }

    @Test
    fun `it should print schemas with correct alignment and truncation`() {
        val iris2 = irisData.addColumn("id") { rowNumber.map { "foo$it".toRegex() } }
        iris2.schema(maxWidth = 20)

        captureOutput { iris2.schema(maxWidth = 20) }.stdout shouldBe """
                DataFrame with 150 observations
                Sepal.Length  [Dbl]    5.1, 4.9, 4.7, 4.6, ...
                Sepal.Width   [Dbl]    3.5, 3, 3.2, 3.1, 3....
                Petal.Length  [Dbl]    1.4, 1.4, 1.3, 1.5, ...
                Petal.Width   [Dbl]    0.2, 0.2, 0.2, 0.2, ...
                Species       [Str]    setosa, setosa, seto...
                id            [Regex]  foo1, foo2, foo3, fo...
                """.trimAndReline()
    }

    @Test
    fun `it should allow to peek into columns`() {
        irisData["Sepal.Length"].toString() shouldBe "Sepal.Length [Dbl][150]: 5.1, 4.9, 4.7, 4.6, 5, 5.4, 4.6, 5, 4.4, 4.9, 5.4, 4.8, 4.8, 4.3, 5.8, 5.7,..."
        irisData["Species"].toString() shouldBe "Species [Str][150]: setosa, setosa, setosa, setosa, setosa, setosa, setosa, setosa, setosa, setosa, ..."

        val users = dataFrameOf("user")(TypeInterfaceTest.User("john", "doe", 33, true))["user"].toString()
        users shouldBe "user [User][1]: User(firstName=john, lastName=doe, age=33, hasSudo=true)"
    }

    @Test
    fun `it should print just first columns and rows`() {
        captureOutput { flightsData.print(maxWidth = 50) }.stdout shouldBe """
                A DataFrame: 336776 x 16
                     year   month   day   dep_time   dep_delay
                 1   2013       1     1        517           2
                 2   2013       1     1        533           4
                 3   2013       1     1        542           2
                 4   2013       1     1        544          -1
                 5   2013       1     1        554          -6
                 6   2013       1     1        554          -4
                 7   2013       1     1        555          -5
                 8   2013       1     1        557          -3
                 9   2013       1     1        557          -3
                10   2013       1     1        558          -2
                and 336766 more rows, and and 11 more variables:
                arr_delay, carrier, tailnum, flight, origin, dest,
                air_time, distance, hour, minute
                """.trimAndReline()
    }

    @Test
    fun `it should print an empty dataframe as such`() {
        captureOutput { emptyDataFrame().print() }.stdout shouldBe """A DataFrame: 0 x 0"""

        captureOutput {
            irisData.filter { it["Species"] eq "foo" }.print()
//            irisData.print()
        }.stdout shouldBe """A DataFrame: 0 x 5
        Sepal.Length            Sepal.Width           Petal.Length            Petal.Width
and 1 more variables: Species""".trimAndReline()
    }


    @Test
    fun `schema should also work for larger tables`() {
        // to prevent regressions of: `schema()` should no throw memory exception #53
        flightsData.schema()
    }
}


class GroupedDataTest {

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
        (sleepData.groupBy("vore") as GroupedDataFrame).groupedBy().nrow shouldBe 5

        // 2) test multi-attribute grouping with NA in one or all attributes
        //        (sleepData.groupBy("vore") as GroupedDataFrame).groups().nrow shouldBe 6
        //todo implement me
    }


    @Test
    fun `distinct avoids hashCode collision`() {
        val dataFrame = dataFrameOf("a", "b", "c")(
                3, 263, 5,
                3, 325, 6,
                5, 201, 1,
                5, 263, 2,
                5, 265, 3,
                5, 325, 4
        )


        println(listOf(3, 263).hashCode())
        println(listOf(5, 201).hashCode())

        println(Arrays.asList(3, 263).hashCode())
        println(Arrays.asList(5, 201).hashCode())


        assertEquals(dataFrame.rows.toList(), dataFrame.distinct("a", "b").rows.toList())
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
                //                .select({ range("year", "day") }, { listOf("arr_delay", "dep_delay") })
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

        val dfA: DataFrame = dataFrameOf(
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


    @Test
    fun `it should group tables with object columns and by object column`() {
        val someUUID = UUID.randomUUID()
        val otherUUID = UUID.randomUUID()

        val df = dataFrameOf("id", "quantity")(
                someUUID, 1,
                otherUUID, 1,
                otherUUID, 2
        )

        // first group by non-object column
        df.groupBy("quantity").apply {
            print()
            groups().size shouldBe 2
        }

        // second group by object column itself
        df.groupBy("id").apply {
            print()
            groups().size shouldBe 2
        }
    }

    @Test
    fun `it should preserve column shape when grouping data-frame without rows`() {
        val df = dataFrameOf(StringCol("foo", emptyList()), IntCol("bar", emptyList()))
        df.print()

        df.groupBy("foo").apply {
            names shouldBe listOf("foo", "bar")
        }
    }

    @Test
    fun `it should correctly use an internal grouping in rowwise`() {
        irisData.rowwise().apply {
            print()

            // does it has the same shape as the original
            nrow shouldBe irisData.nrow
            names shouldBe irisData.names

            schema()

        }
    }


}


class BindRowsTest {

    @Test
    fun `it should add complete rows`() {
        val someDf = dataFrameOf("person", "year", "weight", "sex")(
                "max", 2014, 33.1, "M",
                "max", 2016, null, "M",
                "anna", 2015, 39.2, "F",
                "anna", 2016, 39.9, "F"
        )

        val simpleRow1 = mapOf(
                "person" to "james",
                "year" to 1996,
                "weight" to 54.0,
                "sex" to "M"
        )
        val simpleRow2 = mapOf(
                "person" to "nelll",
                "year" to 1997,
                "weight" to 48.1,
                "sex" to "F"
        )

        someDf.bindRows(simpleRow1, simpleRow2).run {
            nrow shouldBe 6
            ncol shouldBe 4
            rows.elementAt(1)["weight"] shouldBe null
            rows.elementAt(4)["person"] shouldBe "james"
            rows.elementAt(4)["weight"] shouldBe 54.0
            rows.elementAt(5)["person"] shouldBe "nelll"
            rows.elementAt(5)["year"] shouldBe 1997
        }
        // Check that the original has not been modified.
        someDf.nrow shouldBe 4
    }

    @Test
    fun `it should insert NaN for missing columns`() {
        val someDf = dataFrameOf("person", "year", "weight", "sex")(
                "max", 2014, 33.1, "M",
                "max", 2016, null, "M",
                "anna", 2015, 39.2, "F",
                "anna", 2016, 39.9, "F"
        )

        val simpleRow = mapOf(
                "person" to "james",
                "year" to 1996
        )

        someDf.bindRows(simpleRow).run {
            nrow shouldBe 5
            ncol shouldBe 4
            rows.elementAt(1)["weight"] shouldBe null
            rows.elementAt(4)["person"] shouldBe "james"
            rows.elementAt(4)["weight"] shouldBe null
            rows.elementAt(4)["sex"] shouldBe null
        }
        // Check that the original has not been modified.
        someDf.nrow shouldBe 4
    }

    //    @Test(expected = IllegalArgumentException::class)
    @Test
    fun `it should create new columns as needed`() {
        val someDf = dataFrameOf("person", "year", "weight", "sex")(
                "max", 2014, 33.1, "M",
                "max", 2016, null, "M",
                "anna", 2016, 39.9, "F"
        )

        val newColDf = mapOf(
                "person" to "batman",
                "nemesis" to "joker"  // This column does not exist.
        )

        someDf.bindRows(newColDf).run {
            ncol shouldBe 5
            nrow shouldBe 4
        }
    }
}

