package lakehouse.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;

public class Registry {
  public static class PoisonException extends Exception {
    PoisonException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private final String url;
  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  private final ObjectMapper json = new ObjectMapper();
  private final Map<Integer, GenericDatumReader<GenericRecord>> readers = new HashMap<>();

  public Registry(String url) {
    this.url = url;
  }

  public static int schemaId(byte[] bytes) throws PoisonException {
    if (bytes == null || bytes.length < 5 || bytes[0] != 0) {
      throw new PoisonException("not confluent avro", null);
    }
    return ByteBuffer.wrap(bytes, 1, 4).getInt();
  }

  public GenericRecord decode(byte[] bytes) throws PoisonException, IOException, InterruptedException {
    int id = schemaId(bytes);
    GenericDatumReader<GenericRecord> reader = readers.get(id);
    if (reader == null) {
      reader = new GenericDatumReader<>(fetch(id));
      readers.put(id, reader);
    }
    try {
      return reader.read(null, DecoderFactory.get().binaryDecoder(bytes, 5, bytes.length - 5, null));
    } catch (IOException | RuntimeException e) {
      throw new PoisonException("avro decode failed for schema " + id, e);
    }
  }

  private Schema fetch(int id) throws PoisonException, IOException, InterruptedException {
    HttpRequest req = HttpRequest.newBuilder(URI.create(url + "/schemas/ids/" + id)).timeout(Duration.ofSeconds(10)).build();
    for (int attempt = 1; ; attempt++) {
      HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
      if (res.statusCode() == 200) {
        return new Schema.Parser().parse(json.readTree(res.body()).get("schema").asText());
      }
      if (res.statusCode() == 404) {
        throw new PoisonException("unknown schema id " + id, null);
      }
      if (attempt == 5) {
        throw new IOException("schema registry HTTP " + res.statusCode() + " for id " + id);
      }
      Thread.sleep(1000L * attempt);
    }
  }
}
