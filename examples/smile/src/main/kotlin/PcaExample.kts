@file:DependsOn(" com.github.holgerbrandl:kscript-annotations:1.2")
@file:DependsOn("de.mpicbg.scicomp:krangl:0.10.3")
@file:DependsOn("com.github.holgerbrandl:kravis:0.4")
@file:DependsOn("com.github.haifengl:smile-core:1.5.1")
@file:DependsOn("com.github.haifengl:smile-plot:1.5.1")

import krangl.asStrings
import krangl.irisData
import krangl.toDoubleMatrix
import kravis.geomCol
import kravis.geomPoint
import kravis.plot
import smile.math.matrix.JMatrix

fun Array<out DoubleArray>.transpose(): Array<out DoubleArray>? = JMatrix(this).transpose().array()

val irisArray = irisData.remove("Species").toDoubleMatrix().transpose()

val pca = smile.projection.PCA(irisArray)

//barchart
pca.varianceProportion.toList().withIndex()
    .plot(x={ it.index}, y = {it.value})
    .geomCol()
    .yLabel("% Variance Explained")

//val projection = pca.setProjection(2).projection
val rotData = pca.project(irisArray)

// PC1 vs PC2 scatter
rotData.zip(irisData["Species"].asStrings())
    .plot(x={it.first[0]}, y={it.first[1]}, color = {it.second})
    .geomPoint()
