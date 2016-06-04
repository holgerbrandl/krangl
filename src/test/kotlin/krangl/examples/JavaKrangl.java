package krangl.examples;

import kotlin.Pair;
import kotlin.jvm.functions.Function1;
import kotlin.jvm.functions.Function2;
import krangl.ColNames;
import krangl.DataFrame;
import krangl.JoinsKt;
import krangl.TableFormula;

import java.util.ArrayList;
import java.util.List;

import static krangl.ColumnsKt.asDoubles;
import static krangl.Extensions.*;
import static krangl.MathHelpersKt.mean;
import static krangl.TableIOKt.fromCSV;


public class JavaKrangl {

    public static void main(String[] args) {
        DataFrame df = fromCSV(DataFrame.Companion, "/Users/brandl/projects/kotlin/krangl/src/test/resources/krangl/data/msleep.csv");

        DataFrame joinResult = JoinsKt.leftJoin(df, df, "vore", new Pair<String, String>("so", "sdf"));

        select(df, unaryMinus("brain_wt"), new Function1<ColNames, List<Boolean>>() {
            public List<Boolean> invoke(ColNames colNames) {
                return startsWith(colNames, "sleep");
            }
        });

        select(df, unaryMinus("brain_wt"));

        select(df, new ArrayList<String>().toArray(new String[0]));

        select(df, new Function1<ColNames, List<Boolean>>() {

            public List<Boolean> invoke(ColNames colNames) {
                return startsWith(colNames, "sdf");
            }
        });


//        df.getNrow()
        joinResult.summarize(
                new TableFormula("mean_time", new Function2<DataFrame, DataFrame, Object>() {
                    public Object invoke(DataFrame df, DataFrame dataFrame2) {
                        return mean(asDoubles(df.get("time")));
                    }
                }), new TableFormula("median_time", new Function2<DataFrame, DataFrame, Object>() {
                    public Object invoke(DataFrame df, DataFrame dataFrame2) {
                        return mean(asDoubles(df.get("time")));
                    }
                }));


        print(joinResult);
    }
}
