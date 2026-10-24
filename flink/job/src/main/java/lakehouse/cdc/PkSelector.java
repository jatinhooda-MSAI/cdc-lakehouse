package lakehouse.cdc;

import java.util.StringJoiner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.table.data.RowData;

public class PkSelector implements KeySelector<Tuple5<Long, Long, Boolean, Long, RowData>, String> {
  private final RowData.FieldGetter[] getters;

  public PkSelector(RowData.FieldGetter[] getters) {
    this.getters = getters;
  }

  @Override
  public String getKey(Tuple5<Long, Long, Boolean, Long, RowData> e) {
    StringJoiner j = new StringJoiner("|");
    for (RowData.FieldGetter g : getters) {
      j.add(String.valueOf(g.getFieldOrNull(e.f4)));
    }
    return j.toString();
  }
}
