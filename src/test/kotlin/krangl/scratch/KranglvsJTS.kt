package krangl.scratch

import krangl.DataFrame
import krangl.fromCSV
import krangl.glimpse
import tech.tablesaw.api.Table
import tech.tablesaw.kapi.Dataframe
import java.io.File


/**
 * @author Holger Brandl
 */

fun main(args: Array<String>) {
    // https://jtablesaw.wordpress.com/an-introduction/


    val inputFile = File("/Users/brandl/projects/kotlin/misc/tablesaw/data/1950-2014_torn.csv")

//    val tornadoes = Dataframe(Table.read().csv(tornandoCsv))
    val tornadoes = DataFrame.fromCSV(inputFile)

    //    tornadoes.columnNames();
    tornadoes.names

//    tornadoes.shape();
    // DOES NOT EXIST

//    tornadoes.structure();
    tornadoes.glimpse()
//    tornadoes.structure().selectWhere(column("Column Type").isEqualTo("INTEGER"));
//    tornadoes.first(3);
//
//    val month = tornadoes.target.dateColumn("Date").month()
//
//    // todo does this work by ref?
//    tornadoes.target.addColumn(month);
//
//    tornadoes.target.removeColumn("State No");
//
//
//    //parser
//    tornadoes.target.sortOn("-Fatalities");
//
//    tornadoes.target.column("Fatalities").summary().print();
//
//    //filter
//    tornadoes.target.selectWhere(column("Fatalities").isGreaterThan(0));
//    tornadoes.target.selectWhere(column("Width").isGreaterThan(300))
//
//    tornadoes.target.selectWhere(column("Width").isGreaterThan(300))
//
//    tornadoes.target.selectWhere(either(
//            column("Width").isGreaterThan(300),   // 300 yards
//            column("Length").isGreaterThan(10)
//    ))  // 10 miles
//
//    tornadoes.target.select("State", "Date").where(column("Date").isInQ2());
//
//
//    CrossTab.xCount(tornadoes.target, tornadoes.target.categoryColumn("State"), tornadoes.target.shortColumn("Scale"));
//
//    tornadoes.exportToCsv("data/rev_tornadoes_1950-2014.csv");


}