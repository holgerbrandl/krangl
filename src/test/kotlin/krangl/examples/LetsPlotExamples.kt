package krangl.examples

import jetbrains.datalore.plot.PlotHtmlExport
import jetbrains.datalore.plot.PlotHtmlHelper
import jetbrains.datalore.plot.PlotSvgExport
import krangl.DataFrame
import krangl.irisData
import krangl.toMap
import org.jetbrains.letsPlot.export.VersionChecker
import org.jetbrains.letsPlot.geom.geomPoint
import org.jetbrains.letsPlot.intern.GenericAesMapping
import org.jetbrains.letsPlot.intern.toSpec
import org.jetbrains.letsPlot.label.ggtitle
import java.awt.Desktop
import java.io.File
import java.net.URI

fun DataFrame.plt(mapping: GenericAesMapping.() -> Unit = {}) = org.jetbrains.letsPlot.letsPlot(toMap(), mapping)


fun storeAsFile(content: String, extension: String): URI {
    val dir = File(System.getProperty("user.dir"), "lets-plot-images").let {
        it.mkdir()
        it.canonicalPath
    }

    return File(dir, "my_plot.$extension").also {
        it.createNewFile()
        it.writeText(content)
    }.toURI()
}

/**
 * Example of rendering Iris data using JetBrains Lets-Plot library.
 */
fun main() {
    val title = ggtitle("Iris Data - ${irisData.nrow} rows")
    val plot = irisData.plt { x = "Sepal.Width"; y = "Sepal.Length"; color = "Species" } + geomPoint() + title

    PlotHtmlExport.buildHtmlFromRawSpecs(
        plot.toSpec(),
        PlotHtmlHelper.scriptUrl(VersionChecker.letsPlotJsVersion)
    ).also {
        val uri = storeAsFile(it, "html")
        Desktop.getDesktop().browse(uri)
    }

    PlotSvgExport.buildSvgImageFromRawSpecs(plot.toSpec()).also {
        val uri = storeAsFile(it, "svg")
        Desktop.getDesktop().browse(uri)
    }
}