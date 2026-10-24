package lakehouse.cdc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class RowConverter {
  public static final String LSN = "_lsn";
  public static final String OP = "_op";
  public static final String SOURCE_TS = "_source_ts";

  private final String table;
  private final List<Types.NestedField> columns;
  private final Map<Integer, Map<String, Integer>> schemaMap;
  private final int lsnPos;
  private final int opPos;
  private final int tsPos;
  private final Map<Integer, Plan> plans = new HashMap<>();

  public RowConverter(String table, Schema schema, Map<Integer, Map<String, Integer>> schemaMap) {
    this.table = table;
    this.columns = schema.columns();
    this.schemaMap = schemaMap;
    this.lsnPos = position(LSN);
    this.opPos = position(OP);
    this.tsPos = position(SOURCE_TS);
  }

  public GenericRowData convert(int schemaId, GenericRecord image, String op, long lsn, long sourceTsMicros) {
    Plan plan = plans.computeIfAbsent(schemaId, id -> plan(id, image.getSchema()));
    if (plan.drift != null) {
      throw new HaltException("schema-drift", table, plan.drift);
    }
    GenericRowData row = new GenericRowData(columns.size());
    for (int i = 0; i < columns.size(); i++) {
      if (plan.fields[i] != null) {
        row.setField(i, plan.fields[i].apply(image));
      }
    }
    row.setField(lsnPos, lsn);
    row.setField(opPos, StringData.fromString(op));
    row.setField(tsPos, micros(sourceTsMicros));
    return row;
  }

  private int position(String name) {
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).name().equals(name)) {
        return i;
      }
    }
    throw new IllegalStateException(table + " has no " + name + " column");
  }

  private Plan plan(int schemaId, org.apache.avro.Schema avro) {
    Plan plan = new Plan(columns.size());
    Map<String, Integer> mapping = schemaMap.get(schemaId);
    if (mapping == null) {
      plan.drift = "unknown writer schema id " + schemaId;
      return plan;
    }
    Map<Integer, String> byField = new HashMap<>();
    mapping.forEach((name, fieldId) -> byField.put(fieldId, name));
    for (int i = 0; i < columns.size(); i++) {
      Types.NestedField col = columns.get(i);
      String name = byField.get(col.fieldId());
      if (name == null) {
        continue;
      }
      org.apache.avro.Schema.Field f = avro.getField(name);
      if (f == null) {
        plan.drift = "schema id " + schemaId + " has no field " + name;
        return plan;
      }
      Function<Object, Object> c = converter(unwrap(f.schema()), col.type());
      if (c == null) {
        plan.drift = "schema id " + schemaId + " field " + name + " " + describe(f.schema()) + " cannot be written as " + col.type();
        return plan;
      }
      int pos = f.pos();
      plan.fields[i] = r -> {
        Object v = r.get(pos);
        return v == null ? null : c.apply(v);
      };
    }
    return plan;
  }

  static org.apache.avro.Schema unwrap(org.apache.avro.Schema s) {
    if (s.getType() == org.apache.avro.Schema.Type.UNION) {
      for (org.apache.avro.Schema t : s.getTypes()) {
        if (t.getType() != org.apache.avro.Schema.Type.NULL) {
          return t;
        }
      }
    }
    return s;
  }

  private static String describe(org.apache.avro.Schema s) {
    org.apache.avro.Schema u = unwrap(s);
    String name = u.getProp("connect.name");
    return u.getType().getName() + (name == null ? "" : "/" + name)
        + (u.getLogicalType() instanceof LogicalTypes.Decimal d ? "(" + d.getPrecision() + "," + d.getScale() + ")" : "");
  }

  static Function<Object, Object> converter(org.apache.avro.Schema s, Type t) {
    String name = s.getProp("connect.name");
    boolean temporal = name != null && name.startsWith("io.debezium.time.");
    org.apache.avro.Schema.Type at = s.getType();
    switch (t.typeId()) {
      case INTEGER:
        return at == org.apache.avro.Schema.Type.INT && !temporal ? v -> v : null;
      case LONG:
        if (at == org.apache.avro.Schema.Type.LONG && !temporal) {
          return v -> v;
        }
        return at == org.apache.avro.Schema.Type.INT && !temporal ? v -> ((Integer) v).longValue() : null;
      case FLOAT:
        return at == org.apache.avro.Schema.Type.FLOAT ? v -> v : null;
      case DOUBLE:
        if (at == org.apache.avro.Schema.Type.DOUBLE) {
          return v -> v;
        }
        return at == org.apache.avro.Schema.Type.FLOAT ? v -> ((Float) v).doubleValue() : null;
      case BOOLEAN:
        return at == org.apache.avro.Schema.Type.BOOLEAN ? v -> v : null;
      case STRING:
        return at == org.apache.avro.Schema.Type.STRING && !temporal ? v -> StringData.fromString(v.toString()) : null;
      case DECIMAL:
        if (s.getLogicalType() instanceof LogicalTypes.Decimal d) {
          Types.DecimalType dt = (Types.DecimalType) t;
          if (d.getScale() <= dt.scale() && d.getPrecision() - d.getScale() <= dt.precision() - dt.scale()) {
            return v -> DecimalData.fromBigDecimal(
                new BigDecimal(new BigInteger(bytes(v)), d.getScale()).setScale(dt.scale()), dt.precision(), dt.scale());
          }
        }
        return null;
      case DATE:
        return "io.debezium.time.Date".equals(name) ? v -> v : null;
      case TIME:
        if ("io.debezium.time.MicroTime".equals(name)) {
          return v -> (int) Math.floorDiv((Long) v, 1000L);
        }
        return "io.debezium.time.Time".equals(name) ? v -> v : null;
      case TIMESTAMP:
        if (((Types.TimestampType) t).shouldAdjustToUTC()) {
          return "io.debezium.time.ZonedTimestamp".equals(name)
              ? v -> TimestampData.fromInstant(OffsetDateTime.parse(v.toString()).toInstant()) : null;
        }
        if ("io.debezium.time.MicroTimestamp".equals(name)) {
          return v -> micros((Long) v);
        }
        if ("io.debezium.time.Timestamp".equals(name)) {
          return v -> TimestampData.fromEpochMillis((Long) v);
        }
        return "io.debezium.time.NanoTimestamp".equals(name)
            ? v -> TimestampData.fromInstant(Instant.ofEpochSecond(0, (Long) v)) : null;
      case UUID:
        return "io.debezium.data.Uuid".equals(name) ? v -> uuid(v.toString()) : null;
      case BINARY:
        return at == org.apache.avro.Schema.Type.BYTES && s.getLogicalType() == null ? RowConverter::bytes : null;
      default:
        return null;
    }
  }

  static TimestampData micros(long micros) {
    return TimestampData.fromEpochMillis(Math.floorDiv(micros, 1000L), (int) Math.floorMod(micros, 1000L) * 1000);
  }

  private static byte[] uuid(String s) {
    UUID u = UUID.fromString(s);
    return ByteBuffer.allocate(16).putLong(u.getMostSignificantBits()).putLong(u.getLeastSignificantBits()).array();
  }

  static byte[] bytes(Object v) {
    ByteBuffer b = ((ByteBuffer) v).duplicate();
    byte[] out = new byte[b.remaining()];
    b.get(out);
    return out;
  }

  private static class Plan {
    final Function<GenericRecord, Object>[] fields;
    String drift;

    @SuppressWarnings("unchecked")
    Plan(int n) {
      fields = new Function[n];
    }
  }
}
