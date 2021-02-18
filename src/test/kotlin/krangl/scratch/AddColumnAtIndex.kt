package krangl.scratch

import krangl.*

fun main() {
    fun DataFrame.addColumnAtIndex(columnName: String, index: Int, expression: TableExpression): DataFrame {
        return addColumn(columnName) { expression(ec, ec) }
            .select(names.take(index) + listOf(columnName) + names.takeLast(index))
    }

    irisData.addColumnAtIndex("foo", 1) { "krangl rocks!" }.print()
}



