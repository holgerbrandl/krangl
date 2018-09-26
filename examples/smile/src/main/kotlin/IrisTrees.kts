import krangl.*
import smile.classification.AdaBoost
import smile.plot.BarPlot
import kotlin.math.roundToInt

//import kotlin.math.roundToInt


// https://github.com/haifengl/smile/issues/212
//var x = weather.toArray(arrayOfNulls<DoubleArray>(weather.size()))
//var y = weather.toArray(IntArray(weather.size()))


val x = irisData.remove("Species").asDoubleMatrix()
val y = irisData["Species"].asFactor().asType<Factor>().map { factor -> factor?.index!! }.toIntArray()


var forest = AdaBoost(x, y, 200, 4)

forest.importance()

BarPlot(forest.importance())