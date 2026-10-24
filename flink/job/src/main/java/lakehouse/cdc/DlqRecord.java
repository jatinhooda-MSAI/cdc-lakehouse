package lakehouse.cdc;

public class DlqRecord {
  public String topic;
  public int partition;
  public long offset;
  public byte[] key;
  public byte[] value;
  public String error;

  public DlqRecord() {}

  static DlqRecord of(KafkaRecord r, Exception e) {
    DlqRecord d = new DlqRecord();
    d.topic = r.topic;
    d.partition = r.partition;
    d.offset = r.offset;
    d.key = r.key;
    d.value = r.value;
    d.error = e.getClass().getSimpleName() + ": " + e.getMessage();
    return d;
  }
}
