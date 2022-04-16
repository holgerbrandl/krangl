package krangl.test

import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import io.kotest.matchers.types.shouldBeInstanceOf
import krangl.*
import org.junit.Test
import java.sql.DriverManager
import java.sql.Statement
import java.time.LocalDateTime

class BuilderTests {

    @Test
    fun `it should download and cache flights data locally`() {
        if (flightsCacheFile.exists()) flightsCacheFile.delete()
        (flightsData.nrow > 0) shouldBe true
    }

    enum class Engine { Otto, Other }
    data class Car(val name: String, val numCyl: Int?, val engine: Engine)

    @Test
    fun `it should convert objects into data-frames`() {

        val myCars = listOf(
            Car("Ford Mustang", null, Engine.Otto),
            Car("BMW Mustang", 3, Engine.Otto)
        )

        val carsDF = myCars.deparseRecords {
            mapOf(
                "model" to it.name,
                "motor" to it.engine,
                "cylinders" to it.numCyl
            )
        }

        carsDF.nrow shouldBe 2
        carsDF.names shouldBe listOf("model", "motor", "cylinders")

        // use enum order for sorting
        columnTypes(carsDF).print()

        //todo make sure that enum ordinality is used here for sorting
        carsDF.sortedBy { rowNumber }
        //        carsDF.sortedBy { it["motor"] }
        carsDF.sortedBy { it["motor"].asType<Engine>() }
        carsDF.sortedBy { it["motor"].map<Engine> { it.name } }
    }

    @Test
    fun `it should convert object with extractor patterns`() {
        sleepPatterns.deparseRecords(
            "foo" with { awake },
            "bar" with { it.brainwt?.plus(3) }
        ).apply {
            print()
            schema()
            names shouldBe listOf("foo", "bar")
        }
    }


    @Test
    fun `it should coerce lists and iterables to varargs when building inplace data-frames`() {
        dataFrameOf("foo")(listOf(1, 2, 3)).nrow shouldBe 3
        dataFrameOf("foo")(listOf(1, 2, 3).asIterable()).nrow shouldBe 3
        dataFrameOf("foo")(listOf(1, 2, 3).asSequence()).nrow shouldBe 3
    }

    @Test
    fun `it should not allow to create an empty data-frame with dataFrameOf`() {
        // none

//        dataFrameOf(StringCol("user"), DoubleCol("salary"))

        shouldThrow<IllegalArgumentException> {
            dataFrameOf("user", "salary")()
        }

        // but the long syntax must work
        dataFrameOf(StringCol("user", emptyArray()), DoubleCol("salary", emptyArray())).apply {
            nrow shouldBe 0
            ncol shouldBe 2
        }
    }


    @Test
    fun `it should enforce complete data when building inplace data-frames`() {


        // too few
        shouldThrow<IllegalArgumentException> {
            dataFrameOf("user", "salary")(1)
        }

        // too many
        shouldThrow<IllegalArgumentException> {
            dataFrameOf("user", "salary")(1, 2, 3)
        }

    }


    @Test
    fun `it should convert row records in a data frame`() {
        val records = listOf(
            mapOf("name" to "Max", "age" to 23),
            mapOf("name" to "Anna", "age" to 42)
        )

        dataFrameOf(records).apply {
            print()
            nrow shouldBe 2
            names shouldBe listOf("name", "age")
        }
    }

    // https://github.com/holgerbrandl/krangl/issues/84
    @Test
    fun `it should support mixed numbers in column`() {
        val sales = dataFrameOf("product", "sales")(
            "A", 32,
            "A", 12.3,
            "A", 24,
            "B", null,
            "B", 44
        )


        sales.apply {
            schema()
            print()

            this["sales"] should beInstanceOf<DoubleCol>()
        }
    }
}


class JsonTests {


    @Test
    fun `it should read json data from url`() {
        val df = DataFrame.fromJson("https://raw.githubusercontent.com/vega/vega/master/docs/data/movies.json")

        df.apply {
            nrow shouldBe 3201
            names.last() shouldBe "IMDB Votes"
        }
    }

    @Test
    fun `it should read json data from json string`() {
        val df = DataFrame.fromJsonString(
            """
            {
                "cars": {
                    "Nissan": [
                        {"model":"Sentra", "doors":4},
                        {"model":"Maxima", "doors":4},
                        {"model":"Skyline", "doors":2}
                    ],
                    "Ford": [
                        {"model":"Taurus", "doors":4},
                        {"model":"Escort", "doors":4, "seats":5}
                    ]
                }
            }
            """
        )

        df.apply {
            schema()
            print()
            nrow shouldBe 5
            names shouldBe listOf("cars", "model", "doors", "seats")
        }
    }

    @Test
    fun `it should use nearest parent type on json arrays IO`() {
        val df = DataFrame.fromJsonString(
            """
            {
                "cars": {
                    "Nissan": [
                        {"model":"Sentra", "doors":4, "weight":1},
                        {"model":"Maxima", "doors":4, "weight":1.3},
                        {"model":"Skyline", "doors":2}
                    ],
                    "Ford": [
                        {"model":"Taurus", "doors":4, "weight":1.7},
                        {"model":"Escort", "doors":4, "seats":5, "weight":1}
                    ]
                }
            }
            """
        )

        df.apply {
            schema()
            print()
            nrow shouldBe 5
            names shouldBe listOf("cars", "model", "doors", "weight", "seats")
            this["cars"].shouldBeInstanceOf<StringCol>()
            this["model"].shouldBeInstanceOf<StringCol>()
            this["doors"].shouldBeInstanceOf<IntCol>()
            this["weight"].shouldBeInstanceOf<DoubleCol>()
            this["seats"].shouldBeInstanceOf<IntCol>()
        }
    }

    @Test
    fun `it should use nearest parent type on binding objects`() {
        val df = DataFrame.fromJsonString(
            """
            {
                "cars": {
                    "Nissan": [
                        {"model":"Sentra", "doors":4, "weight":1},
                        {"model":"Maxima", "doors":4, "weight":1.3},
                        {"model":"Skyline", "doors":2}
                    ],
                    "Ford": [
                        {"model":"Taurus", "doors":4, "weight":2},
                        {"model":"Escort", "doors":4, "seats":5, "weight":1}
                    ]
                }
            }
            """
        )

        df.apply {
            schema()
            print()
            nrow shouldBe 5
            names shouldBe listOf("cars", "model", "doors", "weight", "seats")
            this["cars"].shouldBeInstanceOf<StringCol>()
            this["model"].shouldBeInstanceOf<StringCol>()
            this["doors"].shouldBeInstanceOf<IntCol>()
            this["weight"].shouldBeInstanceOf<DoubleCol>()
            this["seats"].shouldBeInstanceOf<IntCol>()
        }
    }

    @Test
    fun `it should read incomplete json data from json string`() {
        val df = DataFrame.fromJsonString(
            """
            {
               "Nissan": [
                        {"model":"Sentra", "doors":4},
                        {"model":"Maxima", "doors":4},
                        {"model":"Skyline", "seats":9}
                    ],
            }
            """
        )

        df.apply {
            schema()
            print()
            nrow shouldBe 3
            names shouldBe listOf("_id", "model", "doors", "seats")
        }
    }


    @Test
    fun `it should correctly parse long attributes`() {
        val df = DataFrame.fromJsonString(""" {"test":1612985220914} """)
        df.print()
        df.schema()

        val df3 = DataFrame.fromJsonString(""" {"test":1612985220914, "bar":23} """)
        df3.ncol shouldBe 2
        df3.print()
        df.schema()

        val df2 = DataFrame.fromJsonString("""[{"test":1612985220914},{"test":1612985220914}]""")
        df2.names shouldBe listOf("test")
        df2.schema()
    }


    @Test
    fun `it should convert numerical data-frames to matrices, but should fail for mixed type dfs`() {
        shouldThrow<IllegalArgumentException> { irisData.toDoubleMatrix() }
        shouldThrow<IllegalArgumentException> { irisData.toFloatMatrix() }

        irisData.remove("Species").toDoubleMatrix().apply {
            size shouldBe 4
            first().size shouldBe irisData.nrow
        }
    }
}

class DataBaseTests {

    @Test
    fun `it should parse a table from a database into a dataframe`() {
//        Class.forName("org.postgresql.Driver")

        val conn = DriverManager.getConnection("jdbc:h2:mem:")

        val stmt: Statement = conn.createStatement();


        val setupTmpTable = """
            CREATE TABLE cars(id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255), price INT);
            INSERT INTO cars(name, price) VALUES('Audi', 52642);
            INSERT INTO cars(name, price) VALUES('Mercedes', 57127);
            INSERT INTO cars(name, price) VALUES('Skoda', 9000);
            INSERT INTO cars(name, price) VALUES('Volvo', 29000);
            INSERT INTO cars(name, price) VALUES('Bentley', 350000);
            INSERT INTO cars(name, price) VALUES('Citroen', 21000);
            INSERT INTO cars(name, price) VALUES('Hummer', 41400);
            INSERT INTO cars(name, price) VALUES('Volkswagen', 21600);
        """.trimIndent()

        stmt.execute(setupTmpTable)

        val rs = stmt.executeQuery("SELECT * FROM cars;")

        // convert into DataFrame
        val carsDf: DataFrame = DataFrame.fromResultSet(rs)

        carsDf.apply {
            schema()
            nrow shouldBe 8
            ncol shouldBe 3
        }
    }

    @Test
    fun `it should read dates from a database`() {
//        Class.forName("org.postgresql.Driver")

        val conn = DriverManager.getConnection("jdbc:h2:mem:")

        val stmt: Statement = conn.createStatement();


        val setupTmpTable = """
            CREATE TABLE birthdays(id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR  (255), birthday TIMESTAMP );
            
            INSERT INTO birthdays(name, birthday) VALUES('Max', '2021-01-01 00:00:00');
            INSERT INTO birthdays(name, birthday) VALUES('Anna', '2021-01-01 12:00:00');
        """.trimIndent()

        stmt.execute(setupTmpTable)

        val rs = stmt.executeQuery("SELECT * FROM birthdays;")

        // convert into DataFrame
        val birthdaysDf: DataFrame = DataFrame.fromResultSet(rs)

        birthdaysDf.apply {
            schema()
            nrow shouldBe 2
            ncol shouldBe 3
            get("BIRTHDAY").values().first() should beInstanceOf<LocalDateTime>()
        }
    }
}
