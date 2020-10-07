@file:MavenRepository("repo1", "https://jitpack.io")


@file:DependsOn("de.mpicbg.scicomp:krangl:0.14")
@file:DependsOn("com.github.holgerbrandl:kravis:0.5")
@file:DependsOn("com.github.haifengl:smile-core:1.5.1")
@file:DependsOn("com.github.haifengl:smile-plot:1.5.1")


import krangl.*
import krangl.experimental.oneHot
import kravis.*
import kravis.OrderUtils.reorder
import smile.data.AttributeDataset
import smile.data.NominalAttribute
import smile.data.NumericAttribute
import smile.feature.OneHotEncoder
import smile.math.matrix.JMatrix
import smile.plot.BarPlot.plot
import java.awt.Dimension
import javax.swing.JFrame


//import kotlin.math.roundToInt


// https://github.com/haifengl/smile/issues/212
//var x = weather.toArray(arrayOfNulls<DoubleArray>(weather.size()))
//var y = weather.toArray(IntArray(weather.size()))


fun Array<out DoubleArray>.transpose(): Array<out DoubleArray>? = JMatrix(this).transpose().array()


object MatrixModel {
    @JvmStatic
    fun main(args: Array<String>) {
        val regressor = "Sepal.Length"
        val trainingData = irisData.remove(regressor)
        val x = trainingData.toDoubleMatrix().transpose()
        val y = irisData[regressor].asDoubles().filterNotNull().toDoubleArray()


        var gtb = smile.regression.GradientTreeBoost(x, y, 100)


        //Although some algorithms such as neural network needs the one-hot encoding for categorical data, many algorithms such as decision tree and random forest in our implementation don't need it. We can handle categorical data natively in smile. Thanks!
        val importance = gtb.importance().zip(trainingData.names).asDataFrame()
            .rename("first" to "importance", "second" to "feature")

        importance
            .plot(x = reorder("feature", "importance"), y = "importance")
            .geomBar(stat = Stat.identity)


        // https://haifengl.github.io/smile/visualization.html
        gtb.importance().zip(trainingData.names).asDataFrame()


        // from https://github.com/haifengl/smile/blob/master/demo/src/main/java/smile/demo/clustering/SpectralClusteringDemo.java
        val plot = plot(gtb.importance())

        val f = JFrame("Spectral Clustering")
        f.size = Dimension(1000, 1000)
        f.setLocationRelativeTo(null)
        f.defaultCloseOperation = JFrame.EXIT_ON_CLOSE
        f.contentPane.add(plot)
        f.isVisible = true


        val prediction = gtb.predict(y)

        val irisData2: DataFrame = irisData.addColumn("predicted") { prediction }

        irisData.plot(x = regressor, y = "predicted").geomPoint().show()

    }
}

object AttributeModel {

    @JvmStatic
    fun main(args: Array<String>) {
        val regressor = "Sepal.Length"


        // build another model but using attributes
        // see https://haifengl.github.io/smile/data.html

        val attributes = irisData.cols.map {
            when (it) {
                is DoubleCol -> NumericAttribute(it.name)
                is StringCol -> NominalAttribute(it.name)
                is BooleanCol -> NominalAttribute(it.name)
                else -> {
                    TODO()
                }
            }
        }


        val trainDataset = AttributeDataset("foo", attributes.toTypedArray())

        //https://github.com/jtablesaw/tablesaw/issues/151
        irisData.rows.forEach {
            val row: List<Double> = it.toList().zip(attributes).map { (data, attr) ->
                when (attr) {
                    is NumericAttribute -> data.second as Double
//                is StringAttribute -> attr.valueOf(data.second as String)
//                is NominalAttribute ->
                    else -> {
                        attr.valueOf(data.second.toString())
                    }
                }
            }
            trainDataset.add(row.toDoubleArray())
        }
        val y = irisData[regressor].asDoubles().filterNotNull().toDoubleArray()

        //Although some algorithms such as neural network needs the one-hot encoding for categorical data, many algorithms such as decision tree and random forest in our implementation don't need it. We can handle categorical data natively in smile. Thanks!

        trainDataset.responseAttribute()

//        var gtb = smile.regression.GradientTreeBoos|t(trainDataset.x(), y, 100)
//        var gtb = smile.regression.GradientTreeBoost(trainDataset.attributes(), trainDataset.x(), y, 100)
        // throws Exception in thread "main" java.lang.IllegalStateException: Unsupported attribute type: STRING


        val oneHotEncoder = OneHotEncoder(trainDataset.attributes())
        val ohEncoded = AttributeDataset("foo", oneHotEncoder.attributes())

        var gtb = smile.regression.GradientTreeBoost(ohEncoded.attributes(), ohEncoded.x(), y, 100)


        val importance = gtb.importance()
        gtb.importance()

        // for more demos see https://github.com/haifengl/smile/tree/master/demo/src/main/java/smile/demo

    }

}

object KranglOneHot {
    @JvmStatic
    fun main(args: Array<String>) {
        val regressor = "Sepal.Length"


        val predictors = irisData.oneHot("Species")
        val x = predictors.remove(regressor).toDoubleMatrix()
        val y = irisData[regressor].asDoubles().filterNotNull().toDoubleArray()
        var gtb = smile.regression.GradientTreeBoost(x, y, 100)

        val importance = gtb.importance()

        gtb.importance()
            .zip(predictors.names)
            .asDataFrame()
            .plot(x = "first", y = "second")
            .geomCol()
            .yLabel("Feature Importance")

        irisData.plot(x = regressor, y = "predicted").geomPoint().show()
    }
}