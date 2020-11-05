package krangl.playground

import krangl.*
import kravis.geomBar
import kravis.geomCol
import kravis.geomPoint
import kravis.plot
import smile.projection.PCA


object SmilePCA {
    @JvmStatic
    fun main(args: Array<String>) {
        val pointsArray = arrayOf(
                doubleArrayOf(-1.0, -1.0),
                doubleArrayOf(-1.0, 1.0),
                doubleArrayOf(1.0, 1.0)
        )

        val pca = smile.projection.PCA(pointsArray)
        pca.varianceProportion

        pca.varianceProportion.toList().plot({ it }).geomBar().show()

        pca.varianceProportion.withIndex().plot({ it.index }, { it.value }).geomCol().show()


        val projection = pca.setProjection(2).projection


        projection.array().withIndex().plot(x={ it.value[0] }, y = { it.value[1] }, label = { "PC" + it.index }).geomPoint().show()
//        plotOf(projection.array().withIndex()) {
//            mark(MarkType.text)
//            encoding(x) { value[0] }
//            encoding(y) { value[1] }
//            encoding(text) { "PC" + index }
//        }.render()
    }
}

object IrisPCA {
    @JvmStatic
    fun main(args: Array<String>) {
        val irisArray = irisData.remove("Species").toArray()

        val pca = PCA(irisArray)

        pca.varianceProportion.toList().plot({ it }).geomBar().show()

        pca.varianceProportion.withIndex().plot({ it.index }, { it.value }).geomCol().show()

        val projection = pca.setProjection(2).projection

        projection.array().withIndex().plot(x={ it.value[0] }, y = { it.value[1] }, label = { "PC" + it.index }).geomPoint().show()

        // merge back in the group labels to color scatter
        var pc12 = projection.transpose().array().withIndex().deparseRecords {
            mapOf(
                "index" to it.index + 1,
                "x" to it.value[0],
                "y" to it.value[1])
        }

        pc12 = pc12.leftJoin(irisData.addColumn("index") { rowNumber })

        pc12.plot(x="x", y = "y", color = "Species").geomPoint().show()
    }
}

fun DataFrame.toArray(): Array<DoubleArray> = cols.map {
    it.asDoubles().map {
        it ?: 0.0
    }.toDoubleArray()
}.toTypedArray()

