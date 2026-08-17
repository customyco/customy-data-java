package ai.customy.data;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class CustomyDataClientTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final class Recorder implements CustomyDataClient.Transport {
    final Queue<Integer> statuses = new ArrayDeque<>();
    final List<Map<String, Object>> bodies = new ArrayList<>();
    final List<Map<String, String>> headers = new ArrayList<>();
    Recorder(Integer... values) { statuses.addAll(List.of(values)); }
    @Override public CustomyDataClient.Response send(String url, Map<String, String> header, byte[] body, Duration timeout) throws Exception {
      var payload = MAPPER.readValue(body, new TypeReference<Map<String, Object>>() {});
      bodies.add(payload); headers.add(header);
      int status = statuses.isEmpty() ? 202 : statuses.remove();
      int count = payload.get("batch") instanceof List<?> list ? list.size() : 1;
      var response = status < 300 ? Map.of("accepted", count, "deduplicated", 0, "quarantined", 0, "results", List.of()) : Map.of("error", "temporary");
      return new CustomyDataClient.Response(status, MAPPER.writeValueAsBytes(response));
    }
  }

  private CustomyDataClient client(Recorder recorder, java.util.function.Consumer<CustomyDataClient.Builder> options) {
    var ids = new int[] {0};
    var builder = CustomyDataClient.builder().collectUrl("https://data.customy.ai").writeKey("cdw_test")
        .transport(recorder).retryBase(Duration.ZERO).now(() -> Instant.parse("2026-08-16T00:00:00Z"))
        .idFactory(() -> "message_" + (++ids[0]));
    options.accept(builder); return builder.build();
  }

  @Test void portableSixCallConformance() throws Exception {
    var path = Path.of("conformance/customer-data-v1.json");
    var vectors = MAPPER.readValue(Files.readAllBytes(path), new TypeReference<Map<String, Object>>() {});
    assertEquals(CustomyDataClient.CONFORMANCE_CONTRACT, vectors.get("contract"));
    var recorder = new Recorder(202, 202, 202, 202, 202, 202);
    var sdk = client(recorder, builder -> {});
    @SuppressWarnings("unchecked") var events = (List<Map<String, Object>>) vectors.get("eventTypes");
    events.forEach(sdk::sendEvent);
    assertEquals(List.of("track", "identify", "group", "page", "screen", "alias"), recorder.bodies.stream().map(body -> body.get("type")).toList());
    recorder.bodies.forEach(body -> {
      assertEquals("1.0", body.get("schemaVersion"));
      assertTrue(CustomyDataClientTest.<String>list(vectors.get("forbiddenPayloadKeys")).stream().noneMatch(body::containsKey));
    });
  }

  @Test void retryKeepsMessageId() {
    var recorder = new Recorder(503, 429, 202);
    client(recorder, builder -> {}).track("Checkout Started", Map.of("value", 10), Map.of("anonymousId", "anon_1"));
    assertEquals(3, recorder.bodies.size());
    assertEquals(Set.of("message_1"), recorder.bodies.stream().map(body -> body.get("messageId")).collect(java.util.stream.Collectors.toSet()));
  }

  @Test void redactionTenantBoundaryAndBeforeSend() {
    var recorder = new Recorder();
    var sdk = client(recorder, builder -> builder.redactFields(Set.of("password", "cardNumber")).beforeSend(event -> {
      event.put("traits", new HashMap<>(Map.of("password", "reintroduced"))); return event;
    }));
    sdk.identify(Map.of("password", "secret"), Map.of("userId", "user_1"));
    assertEquals("[REDACTED]", map(recorder.bodies.get(0).get("traits")).get("password"));
    assertThrows(IllegalArgumentException.class, () -> sdk.sendEvent(Map.of("type", "identify", "userId", "u", "organizationId", "forged")));
  }

  @Test void batchRestoresQueueAfterPartialFailure() {
    var sdk = client(new Recorder(202, 503), builder -> builder.maxBatchSize(2).maxRetries(0));
    for (var name : List.of("A", "B", "C")) sdk.enqueue(Map.of("type", "track", "event", name, "anonymousId", "anon_1"));
    assertThrows(CustomyDataClient.DataException.class, sdk::flush);
    assertEquals(4, sdk.enqueue(Map.of("type", "track", "event", "D", "anonymousId", "anon_1")));
  }

  @Test void beforeSendCanBlock() {
    var recorder = new Recorder();
    var sdk = client(recorder, builder -> builder.beforeSend(event -> null));
    assertThrows(CustomyDataClient.DataException.class, () -> sdk.sendEvent(Map.of("type", "track", "event", "Blocked", "anonymousId", "anon_1")));
    assertTrue(recorder.bodies.isEmpty());
  }

  @SuppressWarnings("unchecked") private static Map<String, Object> map(Object value) { return (Map<String, Object>) value; }
  @SuppressWarnings("unchecked") private static <T> List<T> list(Object value) { return (List<T>) value; }
}
