package lakehouse.cdc;

import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public class DlqSerializer implements KafkaRecordSerializationSchema<DlqRecord> {
  private final String topic;

  public DlqSerializer(String topic) {
    this.topic = topic;
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(DlqRecord d, KafkaSinkContext ctx, Long timestamp) {
    ProducerRecord<byte[], byte[]> r = new ProducerRecord<>(topic, null, null, d.key, d.value);
    r.headers()
        .add("error", d.error.getBytes(StandardCharsets.UTF_8))
        .add("source.topic", d.topic.getBytes(StandardCharsets.UTF_8))
        .add("source.partition", String.valueOf(d.partition).getBytes(StandardCharsets.UTF_8))
        .add("source.offset", String.valueOf(d.offset).getBytes(StandardCharsets.UTF_8));
    return r;
  }
}
