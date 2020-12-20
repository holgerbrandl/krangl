# Regression Analysis


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
