package kplyr.test

import io.kotlintest.matchers.have
import io.kotlintest.specs.FlatSpec
import kplyr.*


val sleepData = DataFrame.fromCSV(DataFrame::class.java.getResourceAsStream("data/msleep.csv"))

class CompoundTests : FlatSpec() { init {

    "it" should "summarize sleep data" {

        val groupedSleep = sleepData.filter { it["awake"] gt 3 }.
                apply { glimpse() }.
                mutate("rem_proportion", { it["sleep_rem"] + it["sleep_rem"] }).
                groupBy("vore")


        val insectiMeanREM = sleepData.filter { it["awake"] gt 3 }.
                apply { glimpse() }.
                mutate("rem_proportion", { it["sleep_rem"] + it["sleep_rem"] }).
                groupBy("vore").
                summarize("mean_rem_prop", { it["rem_proportion"].mean(removeNA = true) }).
                filter { it["vore"] eq  "insecti" }.
                row(0)["mean_rem_prop"] as Double


        ((insectiMeanREM - 3.525) < 1E-5) shouldBe true
    }
}
}

/**
require(dplyr)
iris[1, "Species"] <- NA
head(iris)
group_by(iris, Species)
group_by(iris, Species) %>% summarize(mean_length=mean(Sepal.Width))

 */
class JoinTests : FlatSpec() { init {

    "it" should "perform an inner join" {
        val voreInfo = sleepData.groupBy("vore").summarize("vore_mod" to { it["vore"].asStrings().first() + "__2" })
        voreInfo.print()

        val sleepWithInfo = joinLeft(sleepData, voreInfo)
//        sleepWithInfo.print()
        sleepWithInfo.glimpse()

        sleepWithInfo.nrow shouldBe sleepData.nrow
        // make sure that by columns don't show up twice
        sleepWithInfo.ncol shouldBe (sleepData.ncol + 1)
//        sleepWithInfo.names should contain "" // todo reenable
    }

    "it" should "add suffices if join column names have duplicates" {
        // allow user to specify suffix
//        TODO()
    }
    "it" should "join calculate cross-product when joining on empty by list" {
//        TODO()
    }

    "it" should "should allow for NA in by attribute-lists" {
        //todo it's more eyefriendly if NA merge tuples come last in the result table. Can we do the same
//        TODO()
    }
}
}

class SelectTest : FlatSpec() { init {

    "it" should "select with regex" {
        sleepData.select({ endsWith("wt") }).ncol shouldBe 2
        sleepData.select({ endsWith("wt") }).ncol shouldBe 2
        sleepData.select({ startsWith("sleep") }).ncol shouldBe 3
        sleepData.select({ oneOf("conservation", "foobar", "order") }).ncol shouldBe 2
    }

    "it" should "select non-existing column" {
        try {
            sleepData.select("foobar")
            fail("foobar should not be selectable")
        } catch(t: Throwable) {
            // todo expect more descriptive exception here. eg. ColumnDoesNotExistException
        }
    }

    "it" should "select no columns" {
        try {
            sleepData.select(listOf())
            fail("should complain about mismatching selector array dimensionality")
        } catch(t: Throwable) {
        }

        sleepData.select(*arrayOf<String>()).ncol shouldBe 0
    }

    "it" should "select same columns twice" {
        // double selection is flattend out as in dplyr:  iris %>% select(Species, Species) %>% glimpse
        sleepData.select("name", "name").ncol shouldBe 1
    }
}
}

class FilterTest : FlatSpec() { init {
    "it" should "head tail and slic should extract data as expextd" {
        // todo test that the right portions were extracted and not just size
        sleepData.head().nrow shouldBe  5
        sleepData.tail().nrow shouldBe  5
        sleepData.slice(1, 3, 5).nrow shouldBe  3
    }

    "it" should "filter in empty table" {
        sleepData
                .filter { it["name"] eq "foo" }
                // refilter on empty one
                .filter { it["name"] eq "bar" }
    }
}
}


class EmptyTest : FlatSpec() { init {
    "it" should "handle  empty (row and column-empty) data-frames in all operations" {
        SimpleDataFrame().apply {
            // structure
            ncol shouldBe 0
            nrow shouldBe 0
            rows.toList() should have size 0
            cols.toList() should have size 0

            // rendering
            glimpse()
            print()

            // core verbs
            select(emptyList())
            filter { BooleanArray(0) }
            mutate("foo", { "bar" })
            summarize("foo" to { "bar" })
            arrange()

            // grouping
            (groupBy() as GroupedDataFrame).groups()
        }
    }
}
}

class Playground : FlatSpec() { init {
    "it" should "regrouping of data" {

    }
}
}


class GroupedDataTest : FlatSpec() { init {

    /** dplyr considers NA as a group and kplyr should do the same

    ```
    require(dplyr)

    iris
    iris$Species[1] <- NA

    ?group_by
    grpdIris <- group_by(iris, Species)
    grpdIris %>% slice(1)
    ```
     */
    "it" should "allow for NA as a group value" {
        // 1) test single attribute grouping with NA
        (sleepData.groupBy("vore") as GroupedDataFrame).groups().nrow shouldBe 5

        // 2) test multi-attribute grouping with NA in one or all attributes
//        (sleepData.groupBy("vore") as GroupedDataFrame).groups().nrow shouldBe 6
        //todo implement me
    }

    "it" should "count group sizes and report distinct rows in a table" {
        // 1) test single attribute grouping with NA
        sleepData.count("vore").ncol shouldBe 2
        sleepData.count("vore").nrow shouldBe 5

        sleepData.distinct("vore", "order").apply {
            print(this)
            nrow shouldBe 32
            ncol shouldBe 11
        }
    }


}
}

