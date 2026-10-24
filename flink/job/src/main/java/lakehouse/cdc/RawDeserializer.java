package lakehouse.cdc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RawDeserializer implements KafkaRecordDeserializationSchema<KafkaRecord> {
  @Override
  public void deserialize(ConsumerRecord<byte[], byte[]> r, Collector<KafkaRecord> out) {
    KafkaRecord k = new KafkaRecord();
    k.topic = r.topic();
    k.partition = r.partition();
    k.offset = r.offset();
    k.timestamp = r.timestamp();
    k.key = r.key();
    k.value = r.value();
    out.collect(k);
  }

  @Override
  public TypeInformation<KafkaRecord> getProducedType() {
    return TypeInformation.of(KafkaRecord.class);
  }
}
