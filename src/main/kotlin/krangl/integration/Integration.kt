package krangl.integration

import krangl.GroupedDataFrame
import krangl.SimpleDataFrame
import org.jetbrains.kotlinx.jupyter.api.HTML
import org.jetbrains.kotlinx.jupyter.api.annotations.JupyterLibrary
import org.jetbrains.kotlinx.jupyter.api.libraries.JupyterIntegration

//https://github.com/Kotlin/kotlin-jupyter/blob/master/libraries/krangl.json
@JupyterLibrary
internal class Integration : JupyterIntegration() {
    override fun Builder.onLoaded() {
        import("krangl.*")
        render<SimpleDataFrame> { HTML(it.toHTML()) }
        render<GroupedDataFrame> { HTML(it.toHTML()) }
    }

    fun krangl.DataFrame.toHTML(limit: Int = 20, truncate: Int = 50): String = with(StringBuilder()) {
        append("<html><body>")
        append("<table><tr>")

        cols.forEach { append("""<th style=\\" text -align:left\\">${it.name}</th>""") }
        append("</tr>")

        rows.take(limit).forEach {
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

        if (limit < rows.count())
            append("<p>... only showing top $limit rows</p>")
        append("</body></html>")
    }.toString()
}