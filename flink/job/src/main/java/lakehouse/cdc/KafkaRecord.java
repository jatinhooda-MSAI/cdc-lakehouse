package lakehouse.cdc;

public class KafkaRecord {
  public String topic;
  public int partition;
  public long offset;
  public long timestamp;
  public byte[] key;
  public byte[] value;

  public KafkaRecord() {}
}
