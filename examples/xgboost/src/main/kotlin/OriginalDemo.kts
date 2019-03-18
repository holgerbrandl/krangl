@file:MavenRepository("repo1", "https://jitpack.io")


@file:DependsOn("com.github.holgerbrandl:kscript-annotations:1.2")
@file:DependsOn("de.mpicbg.scicomp:krangl:0.11")
@file:DependsOn("com.github.holgerbrandl:kravis:0.5")
@file:DependsOn("ml.dmlc:xgboost4j:0.80")

import ml.dmlc.xgboost4j.java.DMatrix
import ml.dmlc.xgboost4j.java.XGBoost


// build xgboost model
//wget --no-check-certificate https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.test
//wget --no-check-certificate https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.train
//wget --no-check-certificate https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/featmap.txt

val trainMat = DMatrix("agaricus.txt.test")
val testMat = DMatrix("agaricus.txt.train")
//val validMat = DMatrix("valid.svm.txt") // todo where is this one?

val params = hashMapOf<String, Any>().apply {
    put("eta", 1.0)
    put("max_depth", 2)
    put("silent", 1)
    put("objective", "binary:logistic")
    put("eval_metric", "logloss")
}

// https://www.kaggle.com/fashionlee/using-xgboost-for-regression
//our_params={'eta':0.1,'seed':0,'subsample':0.8,'colsample_bytree':0.8,'objective':'reg:linear','max_depth':3,'min_child_weight':1}

// explore the training data
testMat.rowNum()
testMat.rowNum()


// Specify a watch list to see model accuracy on data sets
val watches = hashMapOf<String, DMatrix>().apply {
    put("train", trainMat)
    put("test", testMat)
}

val nround = 2
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
var predicts = booster.predict(testMat)

predicts.joinToString(",")
predicts.first().joinToString(",")

// calculate confusion matrix
predicts


//booster.getFeatureScore()
