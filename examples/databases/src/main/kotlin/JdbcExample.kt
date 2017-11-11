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

    // todo consider using coffee db as in
    // val DB_URL = "jdbc:derby:CoffeeDB;create=true"

    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection("jdbc:h2:~/test", "", "")

    val stmt: Statement = conn.createStatement();


    stmt.execute("drop table user");
    stmt.execute("create table user(id int primary key, name varchar(100))");
    stmt.execute("insert into user values(1, 'hello')");
    stmt.execute("insert into user values(2, 'world')");
    val rs = stmt.executeQuery("select * from user");


    // convert into DataFrame
    val fromResultSet: DataFrame = DataFrame.fromResultSet(rs)
    fromResultSet.print()


//    stmt.close();
}

