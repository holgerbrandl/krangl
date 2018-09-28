import krangl.*
import ml.dmlc.xgboost4j.java.DMatrix
import ml.dmlc.xgboost4j.java.XGBoost

//import kotlin.math.roundToInt


// https://github.com/haifengl/smile/issues/212
//var x = weather.toArray(arrayOfNulls<DoubleArray>(weather.size()))
//var y = weather.toArray(IntArray(weather.size()))


val x = irisData.remove("Species").toDoubleMatrix()
val y = irisData["Species"].asFactor().asType<Factor>().map { factor -> factor?.index!! }.toIntArray()


// build xgboost model
val trainMat = DMatrix("train.svm.txt")
val testMat = DMatrix("train.svm.txt")
val validMat = DMatrix("valid.svm.txt")

val params = object : HashMap<String, Any>() {
    init {
        put("eta", 1.0)
        put("max_depth", 2)
        put("silent", 1)
        put("objective", "binary:logistic")
        put("eval_metric", "logloss")
    }
}

// Specify a watch list to see model accuracy on data sets
val watches = object : HashMap<String, DMatrix>() {
    init {
        put("train", trainMat)
        put("test", validMat)
    }
}
val nround = 2
val booster = XGBoost.train(trainMat, params, nround, watches, null, null)

// build feature map
// https://stackoverflow.com/questions/37627923/how-to-get-feature-importance-in-xgboost
// dump with feature map
//        booster.getFeatureScore()
//        booster.imp
val model_dump_with_feature_map = booster.getModelDump("featureMap.txt", false)

//        predict on test set
var dtest = DMatrix("test.svm.txt")
// predict
var predicts = booster.predict(dtest)
booster.getFeatureScore()