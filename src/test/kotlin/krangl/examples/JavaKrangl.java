package krangl.examples;

import kotlin.Pair;
import kotlin.jvm.functions.Function1;
import kotlin.jvm.functions.Function2;
import krangl.*;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;

import static krangl.ColumnsKt.asDoubles;
import static krangl.Extensions.*;
import static krangl.MathHelpersKt.mean;
import static krangl.SelectKt.startsWith;
import static krangl.SelectKt.unaryMinus;


public class JavaKrangl {

    public static void main(String[] args) {
        DataFrame df = TableIOKt.fromCSV(DataFrame.Companion,"/Users/brandl/projects/kotlin/krangl/src/test/resources/krangl/data/msleep.csv");

        DataFrame joinResult = JoinsKt.leftJoin(df, df, "vore", new Pair<>(".x", ".y"));

        df.selectByName(colNames -> startsWith(colNames, "sleep"));

        df.selectByName( unaryMinus("brain_wt"));

        df.selectByName("brain_wt");

        df.selectByName(colNames -> startsWith(colNames, "sdf"));


//        df.getNrow()
        joinResult.summarize(
                new ColumnFormula("mean_time", (df1, dataFrame2) -> mean(asDoubles(df1.get("time")))),
                new ColumnFormula("median_time", (df12, dataFrame2) -> mean(asDoubles(df12.get("time"))))
        );

        print(joinResult);
    }
}
