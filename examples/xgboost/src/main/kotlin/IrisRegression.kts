@file:MavenRepository("repo1", "https://jitpack.io")


@file:DependsOn("com.github.holgerbrandl:kscript-annotations:1.2")
@file:DependsOn("de.mpicbg.scicomp:krangl:0.11-SNAPSHOT")
@file:DependsOn("com.github.holgerbrandl:kravis:0.4-SNAPSHOT")
@file:DependsOn("ml.dmlc:xgboost4j:0.80")

import krangl.asType
import krangl.irisData
import krangl.toDoubleMatrix
import kravis.geomPoint
import kravis.plot
import ml.dmlc.xgboost4j.java.DMatrix
import ml.dmlc.xgboost4j.java.XGBoost


data class TwoDArrayDimension(val nrow: Int, val ncol: Int)

fun Array<FloatArray>.dim(): TwoDArrayDimension = TwoDArrayDimension(first().size, size)

fun Array<DoubleArray>.toFloatMatrix(): Array<FloatArray> =
    map { it.map { it.toFloat() }.toFloatArray() }.toTypedArray()

val x = irisData.remove("Species", "Petal.Length").toDoubleMatrix().toFloatMatrix()

val xLong: FloatArray = x.reduce { left, right -> left + right }
xLong.size
val y = irisData["Petal.Length"].asType<Double>().map { it!!.toFloat() }.toFloatArray()
y.size
//x.dim
//val y = irisData["Species"].asFactor().asType<Factor>().map { factor -> factor?.index!! }.toIntArray()


// build xgboost model
//wget --no-check-certificate https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.test
//wget --no-check-certificate https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.train
//wget --no-check-certificate https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/featmap.txt

//val trainMat = DMatrix("agaricus.txt.test")
val trainMat = DMatrix(xLong, x.dim().nrow, x.dim().ncol)
trainMat.label = y


//val testMat = DMatrix("agaricus.txt.train")
//val validMat = DMatrix("valid.svm.txt") // todo where is this one?

val params = hashMapOf<String, Any>().apply {
    //    put("colsample_bytree", 0.4)
//    put("gamma", 0)
//    put("learning_rate", 0.07)
//    put("min_child_weight", 1.5)
//    put("eta", 1.0)
//    put("max_depth", 3)
//    put("silent", 1)
    put("objective", "reg:linear")
    put("eval_metric", "rmse")
}

// https://www.kaggle.com/fashionlee/using-xgboost-for-regression
//our_params={'eta':0.1,'seed':0,'subsample':0.8,'colsample_bytree':0.8,'objective':'reg:linear','max_depth':3,'min_child_weight':1}


//https://www.kaggle.com/pablocastilla/predict-house-prices-with-xgboost-regression
//xgboost.XGBRegressor(colsample_bytree=0.4, gamma=0, learning_rate=0.07, max_depth=3, min_child_weight=1.5, n_estimators=10000, reg_alpha=0.75, reg_lambda=0.45, subsample=0.6, seed=42)

// https://stackoverflow.com/questions/33209391/how-to-use-xgboost-algorithm-for-regression-in-r

// Specify a watch list to see model accuracy on data sets
//our_params={'eta':0.1,'seed':0,'subsample':0.8,'colsample_bytree':0.8,'objective':'reg:linear','max_depth':3,'min_child_weight':1}

val watches = hashMapOf<String, DMatrix>().apply {
    put("train", trainMat)
//    put("test", testMat)
}

// number of boosting iteration =3 would just build a simple 2-step function model
val nround = 10
val booster = XGBoost.train(trainMat, params, nround, watches, null, null)


// build feature map
// https://stackoverflow.com/questions/37627923/how-to-get-feature-importance-in-xgboost
// dump with feature map
//        booster.getFeatureScore()
//        booster.imp
val model_dump_with_feature_map = booster.getModelDump("featmap.txt", false)

//        predict on test set
//var dtest = DMatrix("test.svm.txt")
// predict
var predicts = booster.predict(trainMat)
//unwrap
val predictUnwrapped = predicts.map { it.first() }

//https://www.kaggle.com/fashionlee/using-xgboost-for-regression
//from sklearn.metrics import mean_squared_error
//    import math
//    testScore=math.sqrt(mean_squared_error(y_test.values,y_pred))
//print(testScore)

predicts.joinToString(",")
predicts.first().joinToString(",")

predicts.size

val id = irisData.addColumn("predicted_pet_length") { predictUnwrapped }


id.plot(x = "Petal.Length", y = "predicted_pet_length").geomPoint()


booster.getFeatureScore(null)


