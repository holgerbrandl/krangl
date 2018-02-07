package krangl.examples;

import kotlin.Pair;
import krangl.ColumnFormula;
import krangl.DataFrame;
import krangl.JoinsKt;
import krangl.TableIOKt;

import java.util.HashMap;

import static krangl.ColumnsKt.asDoubles;
import static krangl.Extensions.print;
import static krangl.MathHelpersKt.mean;
import static krangl.SelectKt.startsWith;


public class JavaKrangl {

    public static void main(String[] args) {
        DataFrame df = TableIOKt.readCSV(DataFrame.Companion, "/Users/brandl/projects/kotlin/krangl/src/test/resources/krangl/data/msleep.csv", new HashMap<>());

        DataFrame joinResult = JoinsKt.leftJoin(df, df, "vore", new Pair<>(".x", ".y"));

        df.select(colNames -> startsWith(colNames, "sleep"));

        df.remove("brain_wt");

        df.select("brain_wt");

        df.select(colNames -> startsWith(colNames, "sdf"));


//        df.getNrow()
        joinResult.summarize(
                new ColumnFormula("mean_time", (df1, dataFrame2) -> mean(asDoubles(df1.get("time")))),
                new ColumnFormula("median_time", (df12, dataFrame2) -> mean(asDoubles(df12.get("time"))))
        );

        print(joinResult);
    }
}
