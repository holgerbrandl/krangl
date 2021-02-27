package krangl.test

import io.kotest.matchers.shouldBe
import krangl.*
import krangl.UnequalByHelpers.innerJoin
import org.junit.Test


class JointUtilsTest {

    @Test
    fun `test constrained cartesian products`() {
        // project no longer needed becayse of revised cartesianProduct
        val a = DataFrame.builder("name", "project_id")(
            "Max", "P1",
            "Max", "P2",
            "Tom", "P3"
        )

        val b = DataFrame.builder("title", "project_id")(
            "foo", "P1",
            "some_title", "P2",
            "alt_title", "P2"
        )

        val cp = cartesianProduct(a.remove("project_id"), b)
        //        val cp = cartesianProductWithoutBy(a, b, listOf("project_id"))
        cp.print()

        cp.nrow shouldBe 9
        cp["name"][0] shouldBe "Max"
        cp["name"][1] shouldBe "Max"
        cp["name"][2] shouldBe "Tom"

        cp["title"][0] shouldBe "foo"
        cp["title"][1] shouldBe "foo"
        cp["title"][2] shouldBe "foo"
    }
}

/**
require(dplyr)
iris[1, "Species"] <- NA
head(iris)
group_by(iris, Species)
group_by(iris, Species) %>% summarize(mean_length=mean(Sepal.Width))

 */
class InnerJoinTests {

    @Test
    fun `it should perform an inner join`() {
        val voreInfo = sleepData.groupBy("vore").summarize("vore_mod" to { it["vore"].toStrings().first() + "__2" })
        voreInfo.print()

        val sleepWithInfo = sleepData.leftJoin(voreInfo) // auto detect 'by' here

        //        sleepWithInfo.print()
        sleepWithInfo.schema()

        sleepWithInfo.nrow shouldBe sleepData.nrow
        // make sure that by columns don't show up twice
        sleepWithInfo.ncol shouldBe (sleepData.ncol + 1)

        sleepWithInfo.take().print()
        //        sleepWithInfo.names should contain "" // todo reenable
    }


    @Test
    fun `it should allow to join by all columns`() {
        sleepData.innerJoin(sleepData).names shouldBe sleepData.names
    }


    @Test
    fun `it should allow with actually equal bys in unequal mode`() {
        sleepData.innerJoin(
            sleepData.rename("order" to "new_order"), by = listOf(
                "vore" to "vore",
                "order" to "new_order"
            )
        ).nrow shouldBe 597
    }


    @Test
    fun `it should no-overlap data should still return correct column model`() {
        sleepData.innerJoin(irisData.addColumn("vore", { "foobar" }), by = "vore").apply {
            (names.size > 15) shouldBe true
            nrow shouldBe 0
        }
    }


    @Test
    fun `it should add suffices if join column names have duplicates`() {
        // allojezow user to specify suffix
        val df = (dataFrameOf("foo", "bar"))(
            "a", 2,
            "b", 3,
            "c", 4
        )

        // join on foo
        df.innerJoin(df, by = "foo", suffices = "_1" to "_2").apply {
            //            names should contain element "sdf"
            print()
            (names == listOf("foo", "bar_1", "bar_2")) shouldBe true
        }

        // again but now join on bar. Join columns are expected to come first
        df.innerJoin(df, by = "bar", suffices = "_1" to "_2").apply {
            (names == listOf("bar", "foo_1", "foo_2")) shouldBe true
        }

        // again but now join on nothing
        df.innerJoin(df, by = emptyList(), suffices = "_1" to "_2").apply {
            nrow shouldBe 0
            names shouldBe listOf("foo_1", "bar_1", "foo_2", "bar_2")
        }
    }


    @Test
    fun `it should allow to use different and multiple by columns`() {
        persons.rename("last_name" to "name").innerJoin(weights, by = listOf("name" to "last")).apply {
            nrow shouldBe 2
        }
    }
}


class OuterJoinTest {

    @Test
    fun `it should join calculate cross-product when joining on empty by list`() {
        val dfA = dataFrameOf("foo", "bar")(
            "a", 2,
            "b", 3,
            "c", 4
        )
        // todo should the result be the same as for joinInner with by=emptyList() or should we prevent the empty-join for either of them??)
        dfA.outerJoin(dfA, by = emptyList()).apply {
            print()
            nrow shouldBe 6
            ncol shouldBe 4
            names shouldBe listOf("foo.x", "bar.x", "foo.y", "bar.y")
        }
    }


    fun `it should should allow for NA in by attribute-lists`() {
        //todo it's more eyefriendly if NA merge tuples come last in the result table. Can we do the same
        //        TODO()
    }

    @Test
    fun `it should retain join keys of non-matching LHS records in outer join`() {
        // see https://github.com/holgerbrandl/krangl/issues/94
        val user = dataFrameOf(
            "first_name", "last_name", "age", "weight"
        )(
            "Max", "Doe", 23, 55,
            "Franz", "Smith", 23, 88,
            "Horst", "Keanes", 12, 82
        )

        val pets = dataFrameOf("first_name", "pet")(
            "Max", "Cat",
            "Franz", "Dog",
            // no pet for Horst
            "Uwe", "Elephant", // Uwe is not in user dataframe
        )

        user.outerJoin(pets).apply {
            print()

            filter { it["pet"] eq "Elephant" }["first_name"][0] shouldBe "Uwe"
        }
    }

    @Test
    fun `it should join empty dataframe`() {
        val user = dataFrameOf(
            "first_name", "last_name", "age", "weight"
        )(
            "Max", "Doe", 23, 55,
        )

        val pets = dataFrameOf("first_name", "pet")(
            "Max", "Cat",
            "Franz", "Dog",
        )
        val joinBy = listOf("first_name")

//        user.filter { it["first_name"] eq "Max" }.print()
//        user.filter { it["first_name"] eq "Max" }.leftJoin(pets, joinBy).print()
//        user.filter { it["first_name"] eq "Max" }.outerJoin(pets, joinBy).print()

        // Hans does not exist
        user.filter { it["first_name"] eq "Hans" }.print()
        user.filter { it["first_name"] eq "Hans" }.leftJoin(pets, joinBy).apply {
            print("left join result")
            nrow shouldBe 0
            names shouldBe listOf("first_name", "last_name", "age", "weight", "pet")
        }

        user.filter { it["first_name"] eq "Hans" }.outerJoin(pets, joinBy).apply {
            print("right join result")

            names shouldBe listOf("first_name", "last_name", "age", "weight", "pet")

            nrow shouldBe 2
            get("first_name").values() shouldBe arrayOf("Max", "Franz")
        }
    }
}


class SemiAndAntiJoinTest {

    val dfA = dataFrameOf("foo", "bar")(
        "a", 2,
        "b", 3,
        "c", 4
    )
    val filter = dataFrameOf("foo", "bar")(
        "a", 3.2,
        "a", 1.1,
        "b", 3.0,
        "d", 3.2
    )

    fun `it should join calculate cross-product when joining on empty by list`() {

        // todo should the result be the same as for joinInner with by=emptyList() or should we prevent the empty-join for either of them??)
        dfA.semiJoin(filter, by = "foo").apply {
            nrow shouldBe 2
            ncol shouldBe 2

            // make sure that renaming does not kick in
            names shouldBe listOf("foo", "bar")
        }
    }


    fun `it should should allow for NA in by attribute-lists`() {
        //todo it's more eyefriendly if NA merge tuples come last in the result table. Can we do the same
        //        TODO()
    }
}


// todo write test to use different/incompatible types for merge key columns
// todo test that grouped dataframes can be joined as well

class LeftJoinTest {


    @Test
    fun `it should left join calculate cross-product when joining on empty by list`() {

        // todo should the result be the same as for joinInner with by=emptyList() or should we prevent the empty-join for either of them??)
        // OR: join on nothing lacks semantics, so why not using `cartesianProduct` directly

        //        joinOuter(persons, weights, by = "last" to "name").apply {
        //            nrow shouldBe  9
        //            ncol shouldBe 4
        //            names shouldEqual listOf("foo.x", "bar.x", "foo.y", "bar.y")
        //        }
        //        fail("")
        // todo spec out and implement for v0.9
    }


    @Test
    fun `it should should allow for NA in by attribute-lists`() {
        //todo it's more eyefriendly if NA merge tuples come last in the result table. Can we do the same?
        //        TODO()
    }
}


val persons = dataFrameOf(
    "first_name", "last_name", "age"
)(
    "max", "smith", 53,
    "tom", "doe", 30,
    "eva", "miller", 23
)

val weights = dataFrameOf(
    "first", "last", "weight"
)(
    "max", "smith", 56.3,
    "tom", "doe", null,
    "eva", "meyer", 23.3
)