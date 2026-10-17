# common-proto

Message types shared by every contract. The `.proto` files are the single source of
truth: `protoc` (through `protobuf-maven-plugin`) compiles them into the Java classes
that the Kafka events (`common-events`) and the future gRPC stubs use.

## Layout

```
src/main/proto/matchmaking/
├── common/types.proto      # EventMetadata, GameMode, Region (shared by the events)
└── grpc/                   # service stubs - to come (W13+)
```

Java package `com.victor.matchmaking.proto.common`, one file per type
(`java_multiple_files`). Enums keep a `*_UNSPECIFIED = 0` value because proto3 requires
a zero value; services never publish it (a test in `common-events` enforces that every
domain value maps to a distinct non-zero one).

## Build

Generated at build time, no checked-in code:

```bash
mvn -B -pl common/common-proto generate-sources   # target/generated-sources/protobuf/java
```

`protoc` is resolved per platform through the `os-maven-plugin` extension
(`${os.detected.classifier}`), and pinned to the version the Confluent provider
declares (`protobuf.version` in the parent POM) — mismatched `protoc`/`protobuf-java`
versions are a runtime failure waiting to happen.

## Adding a type

1. Add the field/message to a `.proto` file (never renumber or reuse a field number of
   a published contract).
2. `mvn -B install -DskipTests` (regenerates and compiles).
3. If the change belongs to an event, re-run the registrar in `common-events` — the
   registry rejects it when the change breaks `FULL` compatibility.
