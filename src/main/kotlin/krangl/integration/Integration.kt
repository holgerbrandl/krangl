package krangl.integration

import krangl.DataFrame
import krangl.DataFrameSchema
import krangl.GroupedDataFrame
import krangl.SimpleDataFrame
import org.jetbrains.kotlinx.jupyter.api.HTML
import org.jetbrains.kotlinx.jupyter.api.annotations.JupyterLibrary
import org.jetbrains.kotlinx.jupyter.api.libraries.JupyterIntegration

// main docs
// * https://github.com/Kotlin/kotlin-jupyter/blob/master/docs/libraries.md
// * https://blog.jetbrains.com/kotlin/2021/04/kotlin-kernel-for-jupyter-notebook-v0-9-0/
//https://github.com/Kotlin/kotlin-jupyter/blob/master/libraries/krangl.json

var DISPLAY_MAX_ROWS = 10
var DISPLAY_MAX_CHARS = 50

@JupyterLibrary
internal class Integration : JupyterIntegration() {
    override fun Builder.onLoaded() {
        import("krangl.*")
        render<SimpleDataFrame> { HTML(it.toHTML()) }
        render<GroupedDataFrame> { HTML(it.toHTML()) }
        render<DataFrameSchema> { HTML(it.toHTML()) }
    }

    fun DataFrame.toHTML(title: String="A DataFrame", maxRows: Int = DISPLAY_MAX_ROWS, truncate: Int = DISPLAY_MAX_CHARS): String = with(StringBuilder()) {
        append("<html><body>")



        append("<table><tr>")

        cols.forEach { append("""<th style="text-align:left">${it.name}</th>""") }
        append("</tr>")

        rows.take(maxRows).forEach {
            append("<tr>")
            it.values.map { it.toString() }.forEach {
                val truncated = if (truncate > 0 && it.length > truncate) {
                    if (truncate < 4) it.substring(0, truncate)
                    else it.substring(0, truncate - 3) + "..."
                } else {
                    it
                }
                append("""<td style="text-align:left" title="$it">$truncated</td>""")
            }
            append("</tr>")
        }

        append("</table>")

        // render footer
        append("<p>")
        if (maxRows < rows.count()){
            append("... with ${nrow - maxRows} more rows. ")
        }

        appendLine("Shape: ${nrow} x ${ncol}. ")

        if (this@toHTML is GroupedDataFrame) {
            appendLine("Grouped by ${by.joinToString()} [${groups.size}]")
        }
        append("</p>")


        append("</body></html>")
    }.toString()
}