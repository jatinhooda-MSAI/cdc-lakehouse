package lakehouse.cdc;

import org.apache.flink.runtime.throwable.ThrowableAnnotation;
import org.apache.flink.runtime.throwable.ThrowableType;

@ThrowableAnnotation(ThrowableType.NonRecoverableError)
public class HaltException extends RuntimeException {
  public HaltException(String kind, String table, String detail) {
    super(kind + " table=" + table + " " + detail);
  }

  public HaltException(String kind, String table, String detail, Throwable cause) {
    super(kind + " table=" + table + " " + detail, cause);
  }
}
