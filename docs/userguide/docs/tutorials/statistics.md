## Regression Analysis


Using libaries http://commons.apache.org/proper/commons-math/ and https://github.com/chen0040/java-glm, krangl allows to perform R-like regression analyses.

Example: How to fit a linear regression model per group?

```kotlin
val irisModel = irisData
    .groupBy("Species")
    .summarize("lm") {
        val x = it["Sepal.Length"].asDoubles().filterNotNull().toDoubleArray()
        val y = it["Sepal.Width"].asDoubles().filterNotNull().toDoubleArray()

        val xTransposed = MatrixUtils.createRealMatrix(arrayOf(x)).transpose().data
        SimpleRegression().apply { addObservations(xTransposed, y) }
    }
    .unfold<SimpleRegression>("lm", properties = listOf("intercept", "slope"))
```
```
   Species                                                                   lm       slope   intercept
    setosa   org.apache.commons.math3.stat.regression.SimpleRegression@66133adc       0.798     -0.5694
versicolor   org.apache.commons.math3.stat.regression.SimpleRegression@7bfcd12c       0.319      0.8721
 virginica   org.apache.commons.math3.stat.regression.SimpleRegression@42f30e0a       0.2318      1.446
```


# Classical Machine Learning with `SMILE`

The  https://github.com/haifengl/smile projects contains tons of implementations needed for modern data science. Among others

* **Classification** Support Vector Machines, Decision Trees, AdaBoost, Gradient Boosting, Random Forest, Logistic Regression, Neural Networks, RBF Networks, Maximum Entropy Classifier, KNN, Naïve Bayesian, Fisher/Linear/Quadratic/Regularized Discriminant Analysis.

* **Regression** Support Vector Regression, Gaussian Process, Regression Trees, Gradient Boosting, Random Forest, RBF Networks, OLS, LASSO, Ridge Regression.

* **Feature Selection** Genetic Algorithm based Feature Selection, Ensemble Learning based Feature Selection, Signal Noise ratio, Sum Squares ratio.

* **Clustering** BIRCH, CLARANS, DBScan, DENCLUE, Deterministic Annealing, K-Means, X-Means, G-Means, Neural Gas, Growing Neural Gas, Hierarchical Clustering, Sequential Information Bottleneck, Self-Organizing Maps, Spectral Clustering, Minimum Entropy Clustering.

* **Manifold learning** IsoMap, LLE, Laplacian Eigenmap, t-SNE, PCA, Kernel PCA, Probabilistic PCA, GHA, Random Projection, MDS

* **Nearest Neighbor Search** BK-Tree, Cover Tree, KD-Tree, LSH.

* **Sequence Learning** Hidden Markov Model, Conditional Random Field.

* **Natural Language Processing** Tokenizer, Keyword Extractor, Stemmer, POS Tagging, Relevance Ranking


## Example

It's easy to use `krangl` and `smile` to build data science workflows. Here is an example for doing a PCA.

```kotlin
val irisArray = irisData.remove("Species").toArray()

//barchart
pca.varianceProportion.withIndex().plot({ it.index }, { it.value }).geomCol().show()

val projection = pca.setProjection(2).projection

// merge back in the group labels to color scatter
var pc12 = projection.transpose().array().withIndex().deparseRecords {
    mapOf(
        "index" to it.index + 1,
        "x" to it.value[0],
        "y" to it.value[1])
}

pc12 = pc12.leftJoin(irisData.addColumn("index") { rowNumber })

pc12.plot(x="x", y = "y", color = "Species").geomPoint().show()
```

