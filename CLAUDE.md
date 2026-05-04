# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands
- Build: `mvn clean package`
- Build without tests: `mvn clean package -DskipTests`
- Run tests: `mvn test`
- Run single test: `mvn test -Dtest=TestClassName` or `mvn test -Dtest=TestClassName#testMethodName`
- Run specific service:
  - Ingestion: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.14.0-shaded.jar com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer`
  - Query: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.14.0-shaded.jar com.ospreydcs.dp.service.query.server.QueryGrpcServer`
  - Annotation: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.14.0-shaded.jar com.ospreydcs.dp.service.annotation.server.AnnotationGrpcServer`

## Architecture Overview
This is a Data Platform service implementation with three main services:
- **Ingestion Service**: Handles data ingestion with high-performance streaming APIs and comprehensive validation
- **Query Service**: Provides time-series data retrieval and metadata queries
- **Annotation Service**: Manages data annotations, datasets, data exports, PV metadata, and machine configurations

### Service Framework Pattern
Each service follows a consistent architecture:
1. **gRPC Server**: Entry point extending `GrpcServerBase`
2. **Service Implementation**: Implements gRPC service methods, extends protobuf-generated stubs
3. **Handler**: Manages request queue and worker threads, extends `QueueHandlerBase`
4. **Jobs**: Process individual requests asynchronously, extend `HandlerJob`
5. **Database Client**: MongoDB interface for persistence operations
6. **Dispatchers**: Send responses back to clients, extend `Dispatcher`

### Key Components by Service
- **Ingestion**: `ingest.server.IngestionGrpcServer` → `ingest.service.IngestionServiceImpl` → `ingest.handler.mongo.MongoIngestionHandler`
- **Query**: `query.server.QueryGrpcServer` → `query.service.QueryServiceImpl` → `query.handler.mongo.MongoQueryHandler`
- **Annotation**: `annotation.server.AnnotationGrpcServer` → `annotation.service.AnnotationServiceImpl` → `annotation.handler.mongo.MongoAnnotationHandler`

## Multi-Project Structure
The Data Platform consists of two related projects:
- **dp-grpc** (`~/dp.fork/dp-java/dp-grpc`): Contains protobuf definitions for all service APIs
- **dp-service** (this project): Java implementations of the services defined in dp-grpc

### gRPC API Evolution
When modifying gRPC APIs:
1. Update protobuf files in `dp-grpc/src/main/proto/`
2. Regenerate Java classes: `mvn clean compile` in dp-grpc
3. Update service implementations in dp-service to match new protobuf signatures
4. Update validation logic in `IngestionValidationUtility` for new column types
5. Follow systematic renaming pattern: Service → Handler → Jobs → Dispatchers → Tests

## MongoDB Collections
- **buckets**: Time-series data storage (main data collection with embedded protobuf serialization)
- **providers**: Registered data providers
- **requestStatus**: Ingestion request tracking
- **dataSets**: Annotation dataset definitions (contains DataBlockDocuments for time ranges and PV names)
- **annotations**: Data annotations (references dataSets and optionally calculations)
- **calculations**: Associated calculation results (embedded CalculationsDataFrameDocuments)
- **pvMetadata**: PV metadata records (pvName unique index, aliases index; tags, attributes, description, modifiedBy, createdAt, updatedAt)
- **configurations**: Machine configuration records (configurationName unique index, category index; tags, attributes, description, modifiedBy, createdAt, updatedAt)
- **configurationActivations**: Time-bounded activations of configurations (clientActivationId unique sparse index; configurationName, internalCategory, startTime, endTime indexes; tags, attributes, description, modifiedBy, createdAt, updatedAt)

### Document Embedding Pattern
MongoDB documents use embedded protobuf serialization:
- `BucketDocument` contains embedded `DataTimestampsDocument` and `DataColumnDocument`
- `CalculationsDocument` contains embedded `CalculationsDataFrameDocument` list
- `DataSetDocument` contains embedded `DataBlockDocument` list
- Protobuf objects serialized to `bytes` field, with convenience fields for queries

### Column Document Class Hierarchy
The ingestion service uses a class hierarchy for MongoDB column document storage:

**Base Classes:**
- **`ColumnDocumentBase`**: Abstract base with `name` and `columnMetadata` fields
- **`ScalarColumnDocumentBase<T>`**: Generic base for scalar columns; holds `List<T> values`
- **`ArrayColumnDocumentBase`**: Base for array columns; binary little-endian serialization
- **`BinaryColumnDocumentBase`**: Base for binary columns (struct, image, serialized data)

**Column Types (all implemented ✅):**
- Scalar: `DoubleColumnDocument`, `FloatColumnDocument`, `Int64ColumnDocument`, `Int32ColumnDocument`, `BoolColumnDocument`, `StringColumnDocument`, `EnumColumnDocument`
- Array: `DoubleArrayColumnDocument`, `FloatArrayColumnDocument`, `Int32ArrayColumnDocument`, `Int64ArrayColumnDocument`, `BoolArrayColumnDocument`
- Binary: `StructColumnDocument` (schemaId), `ImageColumnDocument` (ImageDescriptor), `SerializedDataColumnDocument` (encoding)
- Legacy: `DataColumnDocument` (maintain for backward compatibility)

Each class uses `@BsonDiscriminator(key = "_t", value = "columnType")` and must be registered in `MongoClientBase.getPojoCodecRegistry()`.

**MongoDB POJO Codec Warning:** The codec silently skips any field missing a getter or setter — `insertMany` succeeds but the field is not written. Every instance variable on every registered BSON class must have both getter and setter.

### Column-Level Metadata
All 16 column proto types carry an optional `metadata` field (`ColumnMetadata` with `ColumnProvenance`, tags, and attributes). The ingestion service stores this as `columnMetadata` on `ColumnDocumentBase`. `ColumnDocumentBase.applyMetadataToProto()` restores it on round-trip via reflection. Validation limits: provenance fields ≤ 256 chars; ≤ 20 tags/attributes each ≤ 256 chars.

## Systematic Process for Adding New Protobuf Column Types

Seven steps for adding a new column type end-to-end:
1. **Create Document Class** — choose base class (Scalar/Array/Binary), add `@BsonDiscriminator`, implement abstract methods, add static factory method, check `hasMetadata()` and call `setColumnMetadata()` in factory
2. **Update BucketDocument** — add handling in `BucketDocument.generateBucketsFromRequest()`
3. **Register POJO Class** — add to `MongoClientBase.getPojoCodecRegistry()`
4. **Data Subscription** — add case in `SourceMonitorManager.publishDataSubscriptions()`
5. **Event Subscription** — update `ColumnTriggerUtility` and `DataBuffer` (scalar only; array/binary are targets only)
6. **Test Framework** — add field to `IngestionTestBase.IngestionRequestParams`, update `buildIngestionRequest()` and `GrpcIntegrationIngestionServiceWrapper.verifyIngestionRequestHandling()`
7. **Integration Test** — create `<ColumnType>IT`; scalar: single-PV pattern; array/binary: dual-PV pattern (scalar trigger + array/binary target)

**Known Technical Debt:** `createColumnBuilder()` and `addAllValuesToBuilder()` are defined at `ColumnDocumentBase` level but only apply to scalars. Future refactoring should move them to `ScalarColumnDocumentBase`.

## Export Framework Architecture
The Annotation Service includes a format-specific export framework:
- **Base Classes**: `ExportDataJobBase` → `ExportDataJobAbstractTabular` → `ExportDataJobCsv`, `ExportDataJobExcel`, `ExportDataJobHdf5`
- **Scalar Columns**: Support all formats (CSV, Excel, HDF5) via `toDataColumn()` conversion
- **Array/Binary Columns**: HDF5 only — cannot convert to legacy DataColumn for tabular formats
- **Excel**: `DataExportXlsxFile` uses `XSSFWorkbook` (non-streaming); suitable for ~50K–100K rows
- **Import**: `DataImportUtility.importXlsxData()` in `com.ospreydcs.dp.client.utility`

## Annotation Service CRUD API Pattern

This section documents the standard pattern for implementing new CRUD APIs on the Annotation Service. `PvMetadataIT` and `ConfigurationIT` are the reference implementations.

### Full Implementation Pipeline

```
AnnotationServiceImpl (gRPC stub override)
  → validates request fields
  → calls AnnotationHandlerInterface method
  → MongoAnnotationHandler (enqueues job)
  → XxxJob.execute() (validates, queries/mutates MongoDB, dispatches result)
  → MongoAnnotationClientInterface / MongoSyncAnnotationClient (MongoDB operations)
  → XxxDispatcher (sends gRPC response to StreamObserver)
```

**Stub methods** skip the queue: `AnnotationServiceImpl` responds immediately with `RESULT_STATUS_ERROR` "not yet implemented".

### Step-by-Step: Adding a New API Method

**Step 1 — BSON Document Class** (`common/bson/<entity>/XxxDocument.java`)
- Extend `DpBsonDocumentBase` for entities with tags, attributes, createdAt, updatedAt
- Every field must have both getter and setter (POJO codec silent-drop pitfall)
- Add static factory `fromSaveXxxRequest()` and conversion method `toXxx()`
- Register in `MongoClientBase.getPojoCodecRegistry()`; embedded helper classes before parent

**Step 2 — MongoDB Collection** (`MongoClientBase.java` and `BsonConstants.java`)
- Add `COLLECTION_NAME_XXX` constant to `MongoClientBase`
- Add `BSON_KEY_XXX_*` constants to `BsonConstants`
- In `MongoSyncAnnotationClient.init()`, call `createIndex()` for key fields (idempotent)

**Step 3 — MongoClient interface and implementation**
- Add signatures to `MongoAnnotationClientInterface`
- Implement in `MongoSyncAnnotationClient`
- Add no-op stubs to `MongoAsyncAnnotationClient`

**Step 4 — Dispatcher** (`annotation/handler/mongo/dispatch/XxxDispatcher.java`)
- Extend `Dispatcher`; implement `handleValidationError()`, `handleError()`, `handleResult()`
- Not-found on get/delete → `RESULT_STATUS_REJECT` (not error)

**Step 5 — Job** (`annotation/handler/mongo/job/XxxJob.java`)
- Extend `HandlerJob`; validation in `execute()` (fail-fast), then mongoClient call, then dispatch

**Step 6 — Handler** (`annotation/handler/mongo/MongoAnnotationHandler.java`)
- Add method to `AnnotationHandlerInterface`; implement in `MongoAnnotationHandler` using `executeJob(job)`

**Step 7 — Service Implementation** (`annotation/service/AnnotationServiceImpl.java`)
- Add static `sendXxxResponseReject/Error/Success()` helpers
- Override gRPC stub method; validate, then delegate to handler

### BSON Document Base Class: `DpBsonDocumentBase`

Documents that need tags, attributes, or managed timestamps extend `DpBsonDocumentBase`:
- Inherited: `List<String> tags`, `Map<String,String> attributes`, `Instant createdAt`, `Instant updatedAt`
- **Attributes**: use `AttributesUtility.attributeMapFromList()` / `attributeListFromMap()` to convert to/from proto `List<Attribute>`
- **Timestamps**: use `TimestampUtility.getTimestampFromInstant()` when building proto responses

### Standard Conventions

**Tag normalization:** Lowercase, deduplicated, sorted on save:
```java
List<String> normalizedTags = new ArrayList<>(
    new TreeSet<>(request.getTagsList().stream().map(String::toLowerCase).toList()));
```

**Upsert with `createdAt` preservation:** On first save, set `createdAt = Instant.now()`. On update, preserve `createdAt` and set `updatedAt = Instant.now()`.

**Not-found → RESULT_STATUS_REJECT:** Get/delete that finds no record returns `RESULT_STATUS_REJECT`, not error.

**Stub methods → immediate RESULT_STATUS_ERROR:** Respond in `AnnotationServiceImpl` with "not yet implemented"; no job enqueued.

**Validation in Job.execute():** Call `dispatcher.handleValidationError(new ResultStatus(true, "message"))` and return early for each violation.

**Result wrapper classes:**
- `MongoSaveResult` — document identifier and error state
- `MongoDeleteResult` — deleted document identifier and error state
- `PvMetadataQueryResult` — `List<PvMetadataDocument>` and `String nextPageToken`
- `ConfigurationQueryResult` — `List<ConfigurationDocument>` and `String nextPageToken`
- `ConfigurationActivationQueryResult` — `List<ConfigurationActivationDocument>` and `String nextPageToken`

### Pagination Pattern

```java
int skipOffset = (pageToken == null || pageToken.isEmpty()) ? 0
    : Integer.parseInt(new String(Base64.getDecoder().decode(pageToken)));
// collection.find(filter).sort(...).skip(skipOffset).limit(limit)
String nextPageToken = (skipOffset + results.size() < totalCount)
    ? Base64.getEncoder().encodeToString(String.valueOf(skipOffset + results.size()).getBytes())
    : null;
```

### Query Criteria → MongoDB Filter Pattern

Build a compound `Filters.and()` from criteria list:

| Criterion type | MongoDB filter |
|---|---|
| Exact match | `Filters.eq(field, value)` |
| Prefix match | `Filters.regex(field, "^prefix")` |
| Contains match | `Filters.regex(field, ".*substring.*")` |
| Tags `$in` | `Filters.in(BSON_KEY_TAGS, values)` |
| Attribute key-only | `Filters.exists("attributes." + key)` |
| Attribute key+values | `Filters.in("attributes." + key, values)` |
| Timestamp overlap | `lte(startTime, ts)` AND (`gt(endTime, ts)` OR `exists(endTime, false)`) |

Multiple match types within one criterion are combined with `Filters.or()`.

### Overlap Constraint Pattern (ConfigurationActivation)

`saveConfigurationActivation` enforces that no two activations for the same `configurationName` or `internalCategory` have overlapping time intervals. The `overlapExists()` method in `MongoSyncAnnotationClient` runs two `countDocuments()` queries (one per dimension). The overlap condition for an existing record [S, E] against a new interval [newS, newE] is:
- `existing.startTime < newEndTime` (or newEndTime absent) AND
- `existing.endTime > newStartTime` OR `existing.endTime` absent

The record being updated is excluded from the check via `Filters.ne(clientActivationId, excludeId)`.

`internalCategory` is denormalized from the referenced `Configuration.category` at save time. Category changes on a `Configuration` are blocked if any activations exist for it.

### Adding a New MongoDB Collection

1. Add `COLLECTION_NAME_XXX` to `MongoClientBase`
2. Add `BSON_KEY_XXX_*` constants to `BsonConstants`
3. In `MongoSyncAnnotationClient.init()`, get collection and call `createIndex()` for key fields
4. Store collection reference as instance field on `MongoSyncAnnotationClient`
5. Register `XxxDocument.class` in `MongoClientBase.getPojoCodecRegistry()`

## Code Style Guidelines
- Java 21 is used for this project
- MongoDB is used for persistence with embedded protobuf serialization
- Package structure: `com.ospreydcs.dp.service.<component>`
- Follow existing naming conventions (CamelCase for classes, lowerCamelCase for methods)
- API method implementations follow: Handler → Job → Database Client → Dispatcher pattern
- Jobs named as `<APIMethod>Job`, Dispatchers as `<APIMethod>Dispatcher`
- Error handling uses DpException and structured logging
- Integration tests located in `integration.<service>` packages
- Follow existing patterns for protobuf ↔ MongoDB document conversion
- Result objects use `ResultStatus` class with `isError` (Boolean) and `msg` (String) fields

## API Method Naming Conventions
- `saveXxx` — upsert (create or update) by natural key
- `queryXxx` — search with filter criteria; returns a list, paginated
- `getXxx` — single-record lookup by natural key or alias; returns `RESULT_STATUS_REJECT` if not found
- `deleteXxx` — remove by natural key or alias; returns `RESULT_STATUS_REJECT` if not found
- `patchXxx` / `bulkSaveXxx` — reserved for partial update and bulk operations; implement as stubs ("not yet implemented") until ready
- Legacy "create" references should be updated to "save" when encountered

## Ingestion Validation Framework
`IngestionValidationUtility` performs layered validation:
1. Basic request (provider ID, request ID, frame presence)
2. Timestamps (SamplingClock and TimestampList, ordering checks)
3. Legacy columns (DataColumn, SerializedDataColumn)
4. New columns (all column-oriented types)
5. Cross-cutting (unique PV names across all column types in a frame)

**Constraints:** string values ≤ 256 chars; array dimensions 1–3 (all > 0); ≤ 10M array elements; image ≤ 50MB; struct ≤ 1MB; timestamps non-decreasing, nanos 0–999,999,999; sample count must match timestamp count.

## Performance Benchmarking Framework
Benchmarks in `com.ospreydcs.dp.service.ingest.benchmark`:
- **`BenchmarkIngestDataStream`** / **`BenchmarkIngestDataBidiStream`**: compare `DATA_COLUMN` (legacy), `DOUBLE_COLUMN`, and `SERIALIZED_DATA_COLUMN` strategies
- Use `--double-column` or `--serialized-column` flags; `--help` for usage
- Key parameters: `numThreads=7`, `numStreams=20`, `numRows=1000`, `numColumns=200` (4000 PVs total), `numSeconds=60`

## Testing Strategy
- **Framework**: JUnit 4 (`@Test`, `@Before`, `@After`)
- **Integration Tests**: `src/test/java/com/ospreydcs/dp/service/integration/`
- **Test Base Classes**: `AnnotationTestBase`, `QueryTestBase`, `IngestionTestBase`
- **Test Database**: "dp-test" (cleaned between tests via `MongoTestClient.init()`)
- **Temporary Files**: `@Rule public TemporaryFolder tempFolder = new TemporaryFolder();`

### Annotation Service Test Framework

Integration tests follow a layered structure:
- **`AnnotationTestBase`** — request builders, `*Params` records, and `*ResponseObserver` inner classes for each API method
- **`AnnotationIntegrationTestIntermediate`** — starts the service and wires up the wrapper
- **`GrpcIntegrationAnnotationServiceWrapper`** — `sendAndVerifyXxx()` helpers that send a request, await response, and assert success/failure

**Response observer pattern:** Each `XxxResponseObserver` holds a `CountDownLatch`, `AtomicBoolean isError`, and result list. `onNext()` spawns a thread to process the response and count down the latch; `onError()` sets the error flag and counts down. `await()` uses a 1-minute timeout.

**`sendAndVerifyXxx()` pattern:** Starts a thread to call the async stub, awaits the observer, asserts `isError()`/`getErrorMessage()` for failure cases or extracts and returns the key identifier for success.

**`MongoTestClient` pattern:** Add `findXxx(String key)` following the retry-loop pattern (300 retries × 100ms = 30s max) to handle asynchronous worker-thread insertion.

**Integration test structure** (`PvMetadataIT` and `ConfigurationIT` are reference implementations):
- Extend `AnnotationIntegrationTestIntermediate`
- Group tests by operation: save, query (all criterion types), get, delete, stubs
- Use `sendAndVerify*` wrappers for happy path and error cases
- Use `MongoTestClient.findXxx()` to verify DB state after saves
- For pagination, use `DpAnnotationServiceGrpc.newStub(channel)` directly with an inline `StreamObserver` and `CountDownLatch`

### Ingestion Test Framework
- **`IngestionTestBase.IngestionRequestParams`**: holds a dedicated `List<XxxColumn>` field for each column type
- **`buildIngestionRequest()`**: populates `IngestDataRequest` from params fields
- **`GrpcIntegrationIngestionServiceWrapper.verifyIngestionRequestHandling()`**: verifies all column types via `toProtobufColumn()` round-trip
- **Scalar tests**: single-PV pattern (`DoubleColumnIT`, etc.)
- **Array/Binary tests**: dual-PV pattern — scalar trigger + array/binary target (`DoubleArrayColumnIT`, `StructColumnIT`, etc.)

### Ingestion Validation Test Coverage
- `IngestionValidationUtilityTest` (22 test cases): legacy validation, new column types, duplicate PV names, timestamp integrity

## Continuous Integration
- **GitHub Actions**: `.github/workflows/ci.yml`
- **Multi-Repository Setup**: builds dp-grpc before dp-service
- **Triggers**: pushes/PRs to main/master; manual workflow dispatch
- **Services**: MongoDB 8.0 service container
- **Artifacts**: Surefire and Failsafe test reports
