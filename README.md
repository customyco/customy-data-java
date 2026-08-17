# Customy Data SDK for Java

Java 17+ SDK for governed `track`, `identify`, `group`, `page`, `screen` and
`alias` collection into Customy Data.

```java
var data = CustomyDataClient.builder()
    .collectUrl("https://data.customy.ai")
    .writeKey("cdw_your_source_write_key")
    .redactFields(Set.of("password", "cardNumber"))
    .build();

data.track("Product Viewed", Map.of("sku", "A-1"),
    Map.of("anonymousId", "anon_123"));
```

Tenant scope is resolved exclusively from the source write key. Customy Data
owns collection and governance; Customy Analytics only consumes approved read
models.
