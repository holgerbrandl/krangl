package krangl.test

import io.kotest.matchers.shouldBe
import krangl.DataFrame
import krangl.arrowReader
import krangl.arrowWriter
import krangl.fromJsonString
import org.junit.Test

class ArrowTests {
    @Test
    fun savingToArrow() {
        val df1 = DataFrame.fromJsonString(
            """
            {
                "cars": {
                    "Nissan": [
                        {"model":"Sentra", "doors":4, "weight":1, },
                        {"model":"Maxima", "doors":4, "weight":1.3},
                        {"model":"Leaf", "doors":4, "electrical":true},
                        {"model":"Skyline", "doors":2, "electrical":false}
                    ],
                    "Ford": [
                        {"model":"Taurus", "doors":4, "weight":2, "electrical":false},
                        {"model":"Escort", "doors":4, "seats":5, "weight":1}
                    ],
                    "Tesla": [
                        {"electrical":true}
                    ]
                }
            }
            """
        )
        val data = df1.arrowWriter().toByteArray()
        val df2 = DataFrame.arrowReader().fromByteArray(data)

        df2.shouldBe(df1)
        //Save to file for test reading from another language (Python or R)
        //df1.arrowWriter().toFile("test.arrow")
    }
}
