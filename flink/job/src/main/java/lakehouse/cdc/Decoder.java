package lakehouse.cdc;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringJoiner;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;

public class Decoder extends ProcessFunction<KafkaRecord, Tuple5<Long, Long, Boolean, Long, RowData>> {
  private final String registryUrl;
  private final String table;
  private final String schemaJson;
  private final HashMap<Integer, HashMap<String, Integer>> schemaMap;
  private final String changelogJson;
  private final OutputTag<RowData> changelogTag;
  private final OutputTag<DlqRecord> dlqTag;

  private transient Registry registry;
  private transient RowConverter converter;
  private transient Schema changelog;
  private transient Counter tombstones;
  private transient Counter dlq;

  public Decoder(String registryUrl, String table, Schema schema, HashMap<Integer, HashMap<String, Integer>> schemaMap,
      Schema changelog, OutputTag<RowData> changelogTag, OutputTag<DlqRecord> dlqTag) {
    this.registryUrl = registryUrl;
    this.table = table;
    this.schemaJson = SchemaParser.toJson(schema);
    this.schemaMap = schemaMap;
    this.changelogJson = changelog == null ? null : SchemaParser.toJson(changelog);
    this.changelogTag = changelogTag;
    this.dlqTag = dlqTag;
  }

  @Override
  public void open(OpenContext ctx) {
    registry = new Registry(registryUrl);
    converter = new RowConverter(table, SchemaParser.fromJson(schemaJson), new HashMap<>(schemaMap));
    changelog = changelogJson == null ? null : SchemaParser.fromJson(changelogJson);
    var group = getRuntimeContext().getMetricGroup().addGroup("table", table);
    tombstones = group.counter("tombstones");
    dlq = group.counter("dlq");
  }

  @Override
  public void processElement(KafkaRecord r, Context ctx, Collector<Tuple5<Long, Long, Boolean, Long, RowData>> out)
      throws IOException, InterruptedException {
    if (r.value == null) {
      tombstones.inc();
      return;
    }
    GenericRecord key;
    try {
      key = registry.decode(r.key);
    } catch (Registry.PoisonException e) {
      dlq.inc();
      ctx.output(dlqTag, DlqRecord.of(r, e));
      return;
    }
    GenericRecord env;
    int schemaId;
    try {
      schemaId = Registry.schemaId(r.value);
      env = registry.decode(r.value);
    } catch (Registry.PoisonException e) {
      throw new HaltException("undecodable-value", table, at(r) + " " + e.getMessage(), e);
    }
    String op = String.valueOf(env.get("op"));
    if (!op.equals("c") && !op.equals("u") && !op.equals("d") && !op.equals("r")) {
      throw new HaltException("unsupported-op", table, "op=" + op + " " + at(r));
    }
    GenericRecord source = (GenericRecord) env.get("source");
    long[] seq = sequence(source);
    String snapshot = String.valueOf(source.get("snapshot"));
    boolean initial = op.equals("r") && !snapshot.equals("incremental") && !snapshot.equals("false");
    Object us = source.getSchema().getField("ts_us") == null ? null : source.get("ts_us");
    long tsMicros = us != null ? (Long) us : (Long) source.get("ts_ms") * 1000L;
    GenericRecord image = (GenericRecord) env.get(op.equals("d") ? "before" : "after");
    if (image == null) {
      throw new HaltException("missing-image", table, "op=" + op + " " + at(r));
    }
    GenericRowData row = converter.convert(schemaId, image, op, seq[1], tsMicros);
    row.setRowKind(op.equals("d") ? RowKind.DELETE : RowKind.INSERT);
    Object version = image.getSchema().getField("version") == null ? null : image.get("version");
    long v = version == null ? -1L : ((Number) version).longValue();
    out.collect(Tuple5.of(seq[0], seq[1], initial, v, row));
    if (changelog != null) {
      ctx.output(changelogTag, changelogRow(key, op, seq, version, tsMicros, r));
    }
  }

  static long[] sequence(GenericRecord source) {
    Object lsn = source.get("lsn");
    long change = lsn == null ? -1L : (Long) lsn;
    Object seq = source.getSchema().getField("sequence") == null ? null : source.get("sequence");
    long commit = -1L;
    if (seq != null) {
      String[] parts = seq.toString().replaceAll("[\\[\\]\" ]", "").split(",");
      if (parts.length == 2) {
        commit = parts[0].equals("null") || parts[0].isEmpty() ? -1L : Long.parseLong(parts[0]);
        change = parts[1].equals("null") || parts[1].isEmpty() ? change : Long.parseLong(parts[1]);
      }
    }
    return new long[] {commit, change};
  }

  private RowData changelogRow(GenericRecord key, String op, long[] seq, Object version, long tsMicros, KafkaRecord r) {
    StringJoiner pk = new StringJoiner("|");
    key.getSchema().getFields().forEach(f -> pk.add(String.valueOf(key.get(f.pos()))));
    GenericRowData row = new GenericRowData(changelog.columns().size());
    set(row, "table_name", StringData.fromString(table));
    set(row, "pk", StringData.fromString(pk.toString()));
    set(row, "op", StringData.fromString(op));
    set(row, "commit_lsn", seq[0]);
    set(row, "lsn", seq[1]);
    set(row, "version", version == null ? null : ((Number) version).longValue());
    set(row, "source_ts", RowConverter.micros(tsMicros));
    set(row, "kafka_topic", StringData.fromString(r.topic));
    set(row, "kafka_partition", r.partition);
    set(row, "kafka_offset", r.offset);
    set(row, "kafka_ts", TimestampData.fromEpochMillis(r.timestamp));
    return row;
  }

  private void set(GenericRowData row, String column, Object value) {
    row.setField(changelog.columns().indexOf(changelog.findField(column)), value);
  }

  private static String at(KafkaRecord r) {
    return r.topic + "/" + r.partition + "@" + r.offset;
  }
}
