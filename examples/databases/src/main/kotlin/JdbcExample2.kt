import krangl.*
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement

/**
 * @author Holger Brandl
 */
// refs
// http://makble.com/h2-database-example-in-gradle
// https://jtablesaw.wordpress.com/2016/06/19/new-load-data-from-any-rdbms/

fun main(args: Array<String>) {

    // for coffeedb use
    // val DB_URL = "jdbc:derby:CoffeeDB;create=true"

    Class.forName("org.postgresql.Driver")

//    val conn = DriverManager.getConnection("jdbc:h2:~/test", "", "")
    val conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/", "postgres", "docker")

    val stmt: Statement = conn.createStatement();


//    stmt.execute("drop table user");
    val rs = stmt.executeQuery("select * from servers");


    // convert into DataFrame
    val fromResultSet: DataFrame = DataFrame.fromResultSet(rs)
    fromResultSet.print()


//    stmt.close();
}

