package krangl.test

import io.kotlintest.specs.FlatSpec
import krangl.IntCol

/**
 * @author Holger Brandl
 */
class ColumnTests : FlatSpec() { init {

    "it" should "do correct column arithmetics" {

        (IntCol("", listOf(3)) + 3)[0] shouldBe 6
        (IntCol("", listOf(3)) + 3.0)[0] shouldBe 6.0
        (IntCol("", listOf(3)) + "foo")[0] shouldBe "3foo"
    }
}
}