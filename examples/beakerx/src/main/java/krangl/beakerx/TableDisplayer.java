/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package krangl.beakerx;

import com.twosigma.beakerx.jvm.object.OutputCell;
import com.twosigma.beakerx.table.TableDisplay;
import jupyter.Displayer;
import jupyter.Displayers;
import krangl.DataFrame;

import java.util.Map;

public class TableDisplayer {

    public static void register() {

        Displayers.register(DataFrame.class, new Displayer<DataFrame>() {
            @Override
            public Map<String, String> display(final DataFrame table) {

                new TableDisplay(
                        table.getNrow(),
                        table.getNcol(),
                        table.getNames(),
                        new TableDisplay.Element() {
                            public String get(int columnIndex, int rowIndex) {
                                return table.get(table.getNames().get(columnIndex)).get(rowIndex) + "";
                            }
                        }

                ).display();
                return OutputCell.DISPLAYER_HIDDEN;
            }

        });

    }
}