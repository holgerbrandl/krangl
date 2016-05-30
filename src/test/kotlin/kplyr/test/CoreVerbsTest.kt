package kplyr.test

import io.kotlintest.matchers.have
import io.kotlintest.specs.FlatSpec
import kplyr.*


val sleepData = DataFrame.fromCSV(DataFrame::class.java.getResourceAsStream("data/msleep.csv"))

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


class MutateTest : FlatSpec() { init {
    "it" should "rename columns and preserve their positions" {
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


class SummarizeTest : FlatSpec() { init {
    "it" should "fail if summaries are not scalar values" {
        shouldThrow<NonScalarValueException> {
            sleepData.summarize("foo", { listOf("a", "b", "c") })
        }
        shouldThrow<NonScalarValueException> {
            sleepData.summarize("foo", { BooleanArray(12) })
        }

    }

    "it" should "should allow complex objects as summaries" {
        class Something {
            override fun toString(): String = "Something(${hashCode()}"
        }

        sleepData.groupBy("vore").summarize("foo" to { Something() }, "bar" to { Something() }).print()
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
}
}

