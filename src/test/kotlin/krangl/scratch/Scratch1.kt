package krangl.scratch

import java.io.BufferedReader
import java.io.FileReader

/**
 * Created by brandl on 6/7/16.
 */

fun main(args: Array<String>) {
    BufferedReader(FileReader("sdf")).lineSequence().map { line ->
        TODO()
    }.toList()

    generateSequence() { readLine() }.map { line ->
        TODO()
    }

}