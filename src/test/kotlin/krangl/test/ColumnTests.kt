package krangl.test

import io.kotlintest.matchers.fail
import io.kotlintest.matchers.shouldBe
import krangl.*
import org.junit.Test

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
    fun `allow to negate and invert columns`() {

        (!BooleanCol("foo", listOf(false, true)))[0] shouldBe true

        (-IntCol("foo", listOf(1, 2)))[1] shouldBe -2
        (-DoubleCol("foo", listOf(1.2, 2.0)))[1] shouldBe -2.0


        shouldThrow<UnsupportedOperationException> { (-BooleanCol("foo", listOf(true))) }
        shouldThrow<UnsupportedOperationException> { (!IntCol("foo", listOf(1))) }
        //
        shouldThrow<UnsupportedOperationException> { (!AnyCol("foo", listOf(1))) }
        shouldThrow<UnsupportedOperationException> { (-AnyCol("foo", listOf(1))) }

    }

    @Test
    fun `wrap column name with backticks if necessary`() {
        val regularColumn = BooleanCol("simple_column", listOf(true, false))
        val spaceColumn = BooleanCol("space column", listOf(true, false))

        wrappedNameIfNecessary(regularColumn) shouldBe "simple_column"
        wrappedNameIfNecessary(spaceColumn) shouldBe "`space column`"
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
    else if (e.javaClass.name != T::class.qualifiedName)
        fail("Expected exception ${T::class.qualifiedName} but ${e.javaClass.name} was thrown")
    else
        return e as T
}
