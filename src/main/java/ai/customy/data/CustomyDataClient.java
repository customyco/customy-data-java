package ai.customy.data;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public final class CustomyDataClient {
  public static final String VERSION = "0.1.0";
  public static final String CONFORMANCE_CONTRACT = "customy.customer-data-sdk.conformance.v1";
  private static final Set<String> EVENT_TYPES = Set.of("track", "identify", "group", "page", "screen", "alias");
  private static final Set<String> FORBIDDEN_TENANT_FIELDS = Set.of("tenantId", "organizationId", "projectId", "environmentId");
  private static final Set<Integer> RETRYABLE_STATUSES = Set.of(429, 500, 502, 503, 504);
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  @FunctionalInterface
  public interface Transport {
    Response send(String url, Map<String, String> headers, byte[] body, Duration timeout) throws Exception;
  }

  public record Response(int status, byte[] body) {}

  public static final class DataException extends RuntimeException {
    private final Integer statusCode;
    private final Object response;

    public DataException(String message) { this(message, null, null, null); }
    public DataException(String message, Integer statusCode, Object response) { this(message, statusCode, response, null); }
    public DataException(String message, Throwable cause) { this(message, null, null, cause); }
    private DataException(String message, Integer statusCode, Object response, Throwable cause) {
      super(message, cause);
      this.statusCode = statusCode;
      this.response = response;
    }
    public Integer statusCode() { return statusCode; }
    public Object response() { return response; }
  }

  public static final class Builder {
    private String collectUrl;
    private String writeKey;
    private Transport transport;
    private int maxRetries = 3;
    private Duration retryBase = Duration.ofMillis(250);
    private Duration timeout = Duration.ofSeconds(10);
    private int maxBatchSize = 100;
    private int maxQueueSize = 10_000;
    private Set<String> redactFields = Set.of();
    private UnaryOperator<Map<String, Object>> beforeSend;
    private Supplier<Instant> now = Instant::now;
    private Supplier<String> idFactory = () -> UUID.randomUUID().toString();

    public Builder collectUrl(String value) { collectUrl = value; return this; }
    public Builder writeKey(String value) { writeKey = value; return this; }
    public Builder transport(Transport value) { transport = value; return this; }
    public Builder maxRetries(int value) { maxRetries = Math.max(0, value); return this; }
    public Builder retryBase(Duration value) { retryBase = value; return this; }
    public Builder timeout(Duration value) { timeout = value; return this; }
    public Builder maxBatchSize(int value) { maxBatchSize = Math.min(1_000, Math.max(1, value)); return this; }
    public Builder maxQueueSize(int value) { maxQueueSize = Math.max(1, value); return this; }
    public Builder redactFields(Set<String> value) { redactFields = Set.copyOf(value); return this; }
    public Builder beforeSend(UnaryOperator<Map<String, Object>> value) { beforeSend = value; return this; }
    public Builder now(Supplier<Instant> value) { now = value; return this; }
    public Builder idFactory(Supplier<String> value) { idFactory = value; return this; }
    public CustomyDataClient build() { return new CustomyDataClient(this); }
  }

  private final String collectUrl;
  private final String writeKey;
  private final Transport transport;
  private final int maxRetries;
  private final Duration retryBase;
  private final Duration timeout;
  private final int maxBatchSize;
  private final int maxQueueSize;
  private final Set<String> redactFields;
  private final UnaryOperator<Map<String, Object>> beforeSend;
  private final Supplier<Instant> now;
  private final Supplier<String> idFactory;
  private final ObjectMapper mapper = new ObjectMapper();
  private final List<Map<String, Object>> queue = new ArrayList<>();
  private final ReentrantLock queueLock = new ReentrantLock();
  private final ReentrantLock flushLock = new ReentrantLock();

  private CustomyDataClient(Builder builder) {
    if (builder.collectUrl == null || builder.collectUrl.isBlank() || builder.writeKey == null || builder.writeKey.isBlank()) {
      throw new IllegalArgumentException("collectUrl and writeKey are required");
    }
    collectUrl = builder.collectUrl.replaceAll("/+$", "");
    writeKey = builder.writeKey;
    transport = builder.transport == null ? this::defaultTransport : builder.transport;
    maxRetries = builder.maxRetries;
    retryBase = builder.retryBase;
    timeout = builder.timeout;
    maxBatchSize = builder.maxBatchSize;
    maxQueueSize = builder.maxQueueSize;
    redactFields = builder.redactFields;
    beforeSend = builder.beforeSend;
    now = builder.now;
    idFactory = builder.idFactory;
  }

  public static Builder builder() { return new Builder(); }

  public Map<String, Object> event(Map<String, Object> input) {
    var normalized = deepCopy(input);
    rejectTenantFields(normalized);
    var type = normalized.get("type");
    if (!(type instanceof String) || !EVENT_TYPES.contains(type)) {
      throw new IllegalArgumentException("type must be track, identify, group, page, screen or alias");
    }
    if (!present(normalized.get("userId")) && !present(normalized.get("anonymousId")) && !present(normalized.get("groupId"))) {
      throw new IllegalArgumentException("at least one userId, anonymousId or groupId is required");
    }
    if ("track".equals(type) && !present(normalized.get("event"))) {
      throw new IllegalArgumentException("track calls require an event name");
    }
    normalized.putIfAbsent("messageId", idFactory.get());
    normalized.putIfAbsent("timestamp", DateTimeFormatter.ISO_INSTANT.format(now.get()));
    normalized.putIfAbsent("schemaVersion", "1.0");
    normalized.putIfAbsent("properties", new HashMap<>());
    normalized.putIfAbsent("traits", new HashMap<>());
    normalized.putIfAbsent("consent", new HashMap<>());
    var context = mapValue(normalized.get("context"));
    context.put("library", Map.of("name", "customy-data-java", "version", VERSION));
    normalized.put("context", context);
    redact(normalized);
    if (beforeSend != null) {
      normalized = beforeSend.apply(deepCopy(normalized));
      if (normalized == null) throw new DataException("event blocked by beforeSend");
      normalized = deepCopy(normalized);
      rejectTenantFields(normalized);
      redact(normalized);
    }
    return normalized;
  }

  public Map<String, Object> sendEvent(Map<String, Object> input) { return request("event", event(input)); }
  public Map<String, Object> track(String name, Map<String, Object> properties, Map<String, Object> identity) {
    var payload = identityEvent("track", identity); payload.put("event", name); payload.put("properties", properties); return sendEvent(payload);
  }
  public Map<String, Object> identify(Map<String, Object> traits, Map<String, Object> identity) {
    var payload = identityEvent("identify", identity); payload.put("traits", traits); return sendEvent(payload);
  }
  public Map<String, Object> group(Map<String, Object> traits, Map<String, Object> identity) {
    var payload = identityEvent("group", identity); payload.put("traits", traits); return sendEvent(payload);
  }
  public Map<String, Object> page(Map<String, Object> properties, Map<String, Object> identity) {
    var payload = identityEvent("page", identity); payload.put("properties", properties); return sendEvent(payload);
  }
  public Map<String, Object> screen(Map<String, Object> properties, Map<String, Object> identity) {
    var payload = identityEvent("screen", identity); payload.put("properties", properties); return sendEvent(payload);
  }
  public Map<String, Object> alias(String userId, String previousId, Map<String, Object> identity) {
    var payload = identityEvent("alias", identity); payload.put("userId", userId); payload.put("anonymousId", previousId); payload.put("properties", Map.of("previousId", previousId)); return sendEvent(payload);
  }

  public int enqueue(Map<String, Object> input) {
    var normalized = event(input);
    queueLock.lock();
    try {
      if (queue.size() >= maxQueueSize) throw new DataException("customer data queue is full");
      queue.add(normalized); return queue.size();
    } finally { queueLock.unlock(); }
  }

  public Map<String, Object> flush() {
    flushLock.lock();
    try {
      List<Map<String, Object>> pending;
      queueLock.lock();
      try { pending = new ArrayList<>(queue); queue.clear(); } finally { queueLock.unlock(); }
      if (pending.isEmpty()) return emptyBatch();
      var aggregate = emptyBatch();
      try {
        for (int offset = 0; offset < pending.size(); offset += maxBatchSize) {
          var batch = pending.subList(offset, Math.min(offset + maxBatchSize, pending.size()));
          var response = request("batch", Map.of("batch", batch));
          for (var key : List.of("accepted", "deduplicated", "quarantined")) {
            aggregate.put(key, number(aggregate.get(key)) + number(response.get(key)));
          }
          listValue(aggregate.get("results")).addAll(listValue(response.get("results")));
        }
        return aggregate;
      } catch (RuntimeException error) {
        queueLock.lock();
        try { queue.addAll(0, pending); } finally { queueLock.unlock(); }
        throw error;
      }
    } finally { flushLock.unlock(); }
  }

  public int queueSize() { queueLock.lock(); try { return queue.size(); } finally { queueLock.unlock(); } }

  private Map<String, Object> request(String path, Object payload) {
    try {
      var body = mapper.writeValueAsBytes(payload);
      var headers = Map.of("content-type", "application/json", "user-agent", "customy-data-java/" + VERSION, "x-write-key", writeKey);
      Exception last = null;
      for (int attempt = 0; attempt <= maxRetries; attempt++) {
        try {
          var response = transport.send(collectUrl + "/v1/collect/" + path, headers, body, timeout);
          var parsed = parse(response.body());
          if (response.status() >= 200 && response.status() < 300) return parsed;
          throw new DataException("Customy Data collection failed with HTTP " + response.status(), response.status(), parsed);
        } catch (Exception error) {
          last = error;
          if (attempt >= maxRetries || !retryable(error)) throw asDataException(error);
          Thread.sleep(retryBase.toMillis() * (1L << attempt));
        }
      }
      throw asDataException(last);
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new DataException("collection retry interrupted", error);
    } catch (IOException error) {
      throw new DataException("event serialization failed", error);
    }
  }

  private Response defaultTransport(String url, Map<String, String> headers, byte[] body, Duration timeoutValue) throws Exception {
    var requestBuilder = HttpRequest.newBuilder(URI.create(url)).timeout(timeoutValue).POST(HttpRequest.BodyPublishers.ofByteArray(body));
    headers.forEach(requestBuilder::header);
    var response = HttpClient.newHttpClient().send(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());
    return new Response(response.statusCode(), response.body());
  }

  private Map<String, Object> identityEvent(String type, Map<String, Object> identity) {
    var result = new HashMap<String, Object>(); result.put("type", type); result.putAll(identity); return result;
  }
  private Map<String, Object> deepCopy(Object value) {
    return mapper.convertValue(value, MAP_TYPE);
  }
  private Map<String, Object> parse(byte[] body) throws IOException {
    if (body == null || body.length == 0) return new HashMap<>();
    try { return mapper.readValue(body, MAP_TYPE); }
    catch (IOException error) { return new HashMap<>(Map.of("raw", new String(body))); }
  }
  private void rejectTenantFields(Map<String, Object> value) {
    var found = FORBIDDEN_TENANT_FIELDS.stream().filter(value::containsKey).sorted().toList();
    if (!found.isEmpty()) throw new IllegalArgumentException("tenant scope is derived from the write key; forbidden fields: " + found);
  }
  @SuppressWarnings("unchecked")
  private void redact(Object value) {
    if (value instanceof Map<?, ?> raw) {
      var map = (Map<String, Object>) raw;
      new ArrayList<>(map.entrySet()).forEach(entry -> {
        if (redactFields.contains(entry.getKey())) map.put(entry.getKey(), "[REDACTED]"); else redact(entry.getValue());
      });
    } else if (value instanceof List<?> list) list.forEach(this::redact);
  }
  @SuppressWarnings("unchecked") private Map<String, Object> mapValue(Object value) { return value instanceof Map<?, ?> ? new HashMap<>((Map<String, Object>) value) : new HashMap<>(); }
  @SuppressWarnings("unchecked") private List<Object> listValue(Object value) { return value instanceof List<?> ? (List<Object>) value : new ArrayList<>(); }
  private boolean present(Object value) { return value != null && !"".equals(value); }
  private int number(Object value) { return value instanceof Number number ? number.intValue() : 0; }
  private boolean retryable(Exception error) { return !(error instanceof DataException dataError) || dataError.statusCode() == null || RETRYABLE_STATUSES.contains(dataError.statusCode()); }
  private DataException asDataException(Exception error) { return error instanceof DataException dataError ? dataError : new DataException("Customy Data collection failed", error); }
  private Map<String, Object> emptyBatch() { var result = new HashMap<String, Object>(); result.put("accepted", 0); result.put("deduplicated", 0); result.put("quarantined", 0); result.put("results", new ArrayList<>()); return result; }
}
