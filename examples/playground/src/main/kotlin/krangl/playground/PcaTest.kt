package krangl.playground

import com.github.holgerbrandl.kravis.spec.EncodingChannel.*
import com.github.holgerbrandl.kravis.spec.MarkType
import com.github.holgerbrandl.kravis.spec.plotOf
import krangl.*
import smile.projection.PCA


object SmilePCA {
    @JvmStatic
    fun main(args: Array<String>) {
        val pointsArray = arrayOf(doubleArrayOf(-1.0, -1.0), doubleArrayOf(-1.0, 1.0), doubleArrayOf(1.0, 1.0))

        val pca = smile.projection.PCA(pointsArray)
        pca.varianceProportion

        plotOf(pca.varianceProportion.toList()) {
            encoding(x) { this }
        }.render()

        plotOf(pca.varianceProportion.withIndex()) {
            mark(MarkType.bar)
            encoding(x) { index }
            encoding(y) { this.value }
        }.render()


        val projection = pca.setProjection(2).projection

        plotOf(projection.array().withIndex()) {
            mark(MarkType.text)
            encoding(x) { value[0] }
            encoding(y) { value[1] }
            encoding(text) { "PC" + index }
        }.render()
    }
}

object IrisPCA {
    @JvmStatic
    fun main(args: Array<String>) {
        val pointsArray = arrayOf(doubleArrayOf(-1.0, -1.0), doubleArrayOf(-1.0, 1.0), doubleArrayOf(1.0, 1.0))
        val irisArray = irisData.remove("Species").toArray()

        val pca = PCA(irisArray)
        //        pca.varianceProportion

        plotOf(pca.varianceProportion.toList()) {
            encoding(x) { this }
        }.render()

        plotOf(pca.varianceProportion.withIndex()) {
            mark(MarkType.bar)
            encoding(x) { index }
            encoding(y) { this.value }
        }.render()


        val projection = pca.setProjection(2).projection

        //        plotOf(projection.transpose().array().withIndex()){
        //            mark(MarkType.point)
        //            encoding(x){value[0]}
        //            encoding(y){value[1]}
        ////            encoding(text){ "PC"+index}
        //        }.render()

        //  val df =listOf(1,2,3).asDataFrame()

        // merge back in the group labels to color scatter
        var pc12 = projection.transpose().array().withIndex().asDataFrame {
            mapOf(
                "index" to it.index + 1,
                "x" to it.value[0],
                "y" to it.value[1])
        }
        pc12 = pc12.leftJoin(irisData.addColumn("index") { rowNumber })


        plotOf(pc12) {
            mark(MarkType.point)
            encoding(x, "x")
            encoding(y, "y")
            encoding(color, "Species")
            //            encoding(text){ "PC"+index}
        }.render()
    }
}

fun DataFrame.toArray(): Array<DoubleArray> = cols.map {
    it.asDoubles().map {
        it ?: 0.0
    }.toDoubleArray()
}.toTypedArray()

