package krangl.benchmarking

import tech.tablesaw.api.ColumnType
import tech.tablesaw.api.Table
import tech.tablesaw.io.csv.CsvReadOptions
import java.io.File

/**
 * @author Holger Brandl
 */
fun main(args: Array<String>) {

    """
kshell_from_kscript.sh <(echo '
//DEPS tech.tablesaw:tablesaw-core:0.25.2
')

kscript --interactive <(echo '
//DEPS tech.tablesaw:tablesaw-core:0.25.2
')
"""

    println("loading table")
    val types = arrayOf(ColumnType.STRING, ColumnType.SKIP, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING)
    //            val delimiter = guessDelimiterx()
    val options = CsvReadOptions.builder(File("/Users/brandl/projects/kotlin/krangl/customers.txt")).separator('\t').columnTypes(types).tableName("foo").build()

    val transactions = Table.read().csv(options)
    println("done")

    println(transactions.rowCount())
    println(transactions.first(10).print())
}
