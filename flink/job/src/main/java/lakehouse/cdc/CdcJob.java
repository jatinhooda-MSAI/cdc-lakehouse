package lakehouse.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

public class CdcJob {
  public static void main(String[] args) throws Exception {
    ParameterTool p = ParameterTool.fromArgs(args);
    String namespace = p.get("namespace", "cdc");
    String bootstrap = p.get("bootstrap", "kafka:9092");
    String registry = p.get("registry", "http://schema-registry:8081");
    String group = p.get("group", "cdc-lakehouse");
    boolean guard = p.getBoolean("guard", true);
    int writers = p.getInt("write-parallelism", 2);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(p.getLong("checkpoint-ms", 30000), CheckpointingMode.EXACTLY_ONCE);

    Map<String, String> props = new HashMap<>();
    props.put("uri", p.get("catalog-uri", "http://iceberg-rest:8181"));
    props.put("warehouse", "s3://warehouse/");
    props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
    props.put("s3.endpoint", p.get("s3-endpoint", "http://minio:9000"));
    props.put("s3.path-style-access", "true");
    props.put("s3.access-key-id", "minio");
    props.put("s3.secret-access-key", "minio12345");
    props.put("client.region", "us-east-1");
    CatalogLoader loader = CatalogLoader.rest("iceberg", new org.apache.hadoop.conf.Configuration(), props);
    Catalog catalog = loader.loadCatalog();

    Map<String, List<TableIdentifier>> byTopic = new TreeMap<>();
    TableIdentifier changelogId = null;
    for (TableIdentifier id : catalog.listTables(Namespace.of(namespace))) {
      Map<String, String> tp = catalog.loadTable(id).properties();
      if ("true".equals(tp.get("cdc.changelog"))) {
        changelogId = id;
      } else if (tp.containsKey("cdc.topic")) {
        List<TableIdentifier> ids = byTopic.computeIfAbsent(tp.get("cdc.topic"), k -> new ArrayList<>());
        if (tp.containsKey("cdc.shadow-of")) {
          ids.add(id);
        } else {
          ids.add(0, id);
        }
      }
    }
    if (byTopic.isEmpty()) {
      throw new IllegalStateException("no cdc tables in namespace " + namespace);
    }

    Schema changelogSchema = changelogId == null ? null : catalog.loadTable(changelogId).schema();
    OutputTag<RowData> changelogTag = changelogSchema == null ? null
        : new OutputTag<>("changelog", InternalTypeInfo.of(FlinkSchemaUtil.convert(changelogSchema)));
    OutputTag<DlqRecord> dlqTag = new OutputTag<>("dlq", TypeInformation.of(DlqRecord.class));
    DataStream<RowData> changes = null;
    DataStream<DlqRecord> dead = null;

    for (Map.Entry<String, List<TableIdentifier>> e : byTopic.entrySet()) {
      TableIdentifier primaryId = e.getValue().get(0);
      String name = primaryId.name();
      Table primary = catalog.loadTable(primaryId);
      Schema schema = primary.schema();
      RowType rowType = FlinkSchemaUtil.convert(schema);
      InternalTypeInfo<RowData> type = InternalTypeInfo.of(rowType);

      KafkaSource<KafkaRecord> source = KafkaSource.<KafkaRecord>builder()
          .setBootstrapServers(bootstrap)
          .setTopics(e.getKey())
          .setGroupId(group)
          .setClientIdPrefix(group + "-" + name)
          .setStartingOffsets(OffsetsInitializer.earliest())
          .setDeserializer(new RawDeserializer())
          .setProperty("commit.offsets.on.checkpoint", "true")
          .build();

      TypeInformation<Tuple5<Long, Long, Boolean, Long, RowData>> changeType = Types.TUPLE(Types.LONG, Types.LONG, Types.BOOLEAN, Types.LONG, type);
      SingleOutputStreamOperator<Tuple5<Long, Long, Boolean, Long, RowData>> decoded = env
          .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka " + name).uid("source-" + name)
          .process(new Decoder(registry, name, schema, schemaMap(primary), changelogSchema, changelogTag, dlqTag), changeType)
          .name("decode " + name).uid("decode-" + name);

      List<String> pk = new ArrayList<>();
      List<RowData.FieldGetter> getters = new ArrayList<>();
      for (int i = 0; i < schema.columns().size(); i++) {
        if (schema.identifierFieldIds().contains(schema.columns().get(i).fieldId())) {
          pk.add(schema.columns().get(i).name());
          getters.add(RowData.createFieldGetter(rowType.getTypeAt(i), i));
        }
      }
      DataStream<RowData> applied = decoded
          .keyBy(new PkSelector(getters.toArray(new RowData.FieldGetter[0])))
          .process(new LsnGuard(name, guard), type)
          .name("guard " + name).uid("guard-" + name);

      for (TableIdentifier id : e.getValue()) {
        FlinkSink.forRowData(applied)
            .tableLoader(TableLoader.fromCatalog(loader, id))
            .equalityFieldColumns(pk)
            .upsert(true)
            .writeParallelism(writers)
            .uidPrefix("sink-" + id.name())
            .append();
      }
      if (changelogTag != null) {
        DataStream<RowData> c = decoded.getSideOutput(changelogTag);
        changes = changes == null ? c : changes.union(c);
      }
      DataStream<DlqRecord> d = decoded.getSideOutput(dlqTag);
      dead = dead == null ? d : dead.union(d);
    }

    if (changes != null) {
      FlinkSink.forRowData(changes)
          .tableLoader(TableLoader.fromCatalog(loader, changelogId))
          .writeParallelism(1)
          .uidPrefix("sink-changelog")
          .append();
    }
    dead.sinkTo(KafkaSink.<DlqRecord>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(new DlqSerializer(p.get("dlq-topic", "cdc.dlq")))
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build())
        .name("dlq").uid("dlq");

    env.execute(p.get("job-name", "cdc-lakehouse"));
  }

  private static HashMap<Integer, HashMap<String, Integer>> schemaMap(Table t) throws Exception {
    HashMap<Integer, HashMap<String, Integer>> out = new HashMap<>();
    JsonNode root = new ObjectMapper().readTree(t.properties().getOrDefault("cdc.schema-map", "{}"));
    root.fields().forEachRemaining(e -> {
      HashMap<String, Integer> m = new HashMap<>();
      e.getValue().fields().forEachRemaining(f -> m.put(f.getKey(), f.getValue().asInt()));
      out.put(Integer.parseInt(e.getKey()), m);
    });
    return out;
  }
}
