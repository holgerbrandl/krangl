package krangl.scratch

import krangl.median
import org.apache.commons.math3.stat.StatUtils

val numbers = arrayOf(1.0, 2.0, 3.0)
val numbers2 = arrayOf(1.0, 2.0, 3.0, 4.0)
val numbers3 = arrayOf(1.0)

println("correct")
// from Apache Commons Math
println(StatUtils.percentile(numbers.toDoubleArray(), 50.0))
println(StatUtils.percentile(numbers2.toDoubleArray(), 50.0))
println(StatUtils.percentile(numbers3.toDoubleArray(), 50.0))

println("wrong")

repeat(3) {
    val n = numbers.copyOf()
    n.shuffle()
    println(n.median())
}
println()
repeat(3) {
    val n = numbers2.copyOf()
    n.shuffle()
    println(n.median())
}
println()
println(numbers3.median())