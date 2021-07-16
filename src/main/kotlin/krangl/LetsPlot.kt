import jetbrains.letsPlot.intern.GenericAesMapping
import krangl.DataFrame
import krangl.toMap

/** Plot a data-frame with let-plot. To use this mapping add `implementation("org.jetbrains.lets-plot:lets-plot-kotlin-jvm:3.0.1")` or via `%use lets-plot` when using jupyter. */
fun DataFrame.letsPlot(mapping: GenericAesMapping.() -> Unit = {}) = jetbrains.letsPlot.letsPlot(toMap())


//fun main() {
//    irisData.letsPlot{ x= "Sepal.Width"; y="Sepal.Length"; color="Species"} + geomPoint()
//}
