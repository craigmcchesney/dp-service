# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands
- Build: `mvn clean package`
- Build without tests: `mvn clean package -DskipTests`
- Run tests: `mvn test`
- Run single test: `mvn test -Dtest=TestClassName` or `mvn test -Dtest=TestClassName#testMethodName`
- Run specific service: 
  - Ingestion: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.11.0-shaded.jar com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer`
  - Query: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.11.0-shaded.jar com.ospreydcs.dp.service.query.server.QueryGrpcServer`
  - Annotation: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.11.0-shaded.jar com.ospreydcs.dp.service.annotation.server.AnnotationGrpcServer`

## Architecture Overview
This is a Data Platform service implementation with three main services:
- **Ingestion Service**: Handles data ingestion with high-performance streaming APIs and comprehensive validation
- **Query Service**: Provides time-series data retrieval and metadata queries 
- **Annotation Service**: Manages data annotations, datasets, and data exports

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

### Document Embedding Pattern
MongoDB documents use embedded protobuf serialization:
- `BucketDocument` contains embedded `DataTimestampsDocument` and `DataColumnDocument`
- `CalculationsDocument` contains embedded `CalculationsDataFrameDocument` list
- `DataSetDocument` contains embedded `DataBlockDocument` list
- Protobuf objects serialized to `bytes` field, with convenience fields for queries

### Column Document Class Hierarchy
The ingestion service uses a sophisticated class hierarchy for MongoDB column document storage:

**Base Classes:**
- **`ColumnDocumentBase`**: Abstract base with `name` field and methods for protobuf/MongoDB conversion
- **`ScalarColumnDocumentBase<T>`**: Generic intermediate class for scalar column types

**Scalar Column Implementation Pattern:**
```java
@BsonDiscriminator(key = "_t", value = "columnType")
public class TypeColumnDocument extends ScalarColumnDocumentBase<JavaType> {
    
    // Static factory method
    public static TypeColumnDocument fromTypeColumn(TypeColumn requestColumn) {
        TypeColumnDocument document = new TypeColumnDocument();
        document.setName(requestColumn.getName());
        document.setValues(requestColumn.getValuesList());
        return document;
    }
    
    // Protobuf builder creation
    @Override
    protected Message.Builder createColumnBuilder() {
        return TypeColumn.newBuilder();
    }
    
    // Add values to protobuf builder
    @Override
    protected void addAllValuesToBuilder(Message.Builder builder, List<JavaType> values) {
        ((TypeColumn.Builder) builder).addAllValues(values);
    }
    
    // Convert scalar to DataValue for legacy compatibility
    @Override
    protected DataValue createDataValueFromScalar(JavaType value) {
        return DataValue.newBuilder().setTypeValue(value).build();
    }
    
    // Add column to DataBucket for ingestion response
    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        TypeColumn column = (TypeColumn) toProtobufColumn();
        bucketBuilder.setTypeColumn(column);
    }
}
```

**Scalar Column Type Mappings:**
| Proto Message | Java Generic Type | Document Class | BSON Discriminator |
|--------------|------------------|----------------|-------------------|
| DoubleColumn | `ScalarColumnDocumentBase<Double>` | DoubleColumnDocument | "doubleColumn" |
| FloatColumn | `ScalarColumnDocumentBase<Float>` | FloatColumnDocument | "floatColumn" |
| Int64Column | `ScalarColumnDocumentBase<Long>` | Int64ColumnDocument | "int64Column" |
| Int32Column | `ScalarColumnDocumentBase<Integer>` | Int32ColumnDocument | "int32Column" |
| BoolColumn | `ScalarColumnDocumentBase<Boolean>` | BoolColumnDocument | "boolColumn" |
| StringColumn | `ScalarColumnDocumentBase<String>` | StringColumnDocument | "stringColumn" |
| EnumColumn | `ScalarColumnDocumentBase<Integer>` | EnumColumnDocument | "enumColumn" |

**Benefits of Generic Base Class:**
- **Code Reuse**: `List<T> values` field and common methods inherited from base
- **Type Safety**: Compile-time type checking with generic parameter `<T>`
- **Consistent API**: All scalar columns follow same conversion patterns
- **Memory Efficiency**: Maintains column-oriented storage for high-frequency ingestion
- **Legacy Compatibility**: Generic `toDataColumn()` converts to sample-oriented DataColumn

**Inherited Methods from ScalarColumnDocumentBase:**
- `getValues()` / `setValues()` - Generic value list accessors
- `toDataColumn()` - Converts to legacy DataColumn with DataValue objects
- `getBytes()` - Serializes protobuf column to byte array
- `toProtobufColumn()` - Template method for creating typed protobuf column

## Export Framework Architecture
The Annotation Service includes a sophisticated export framework:
- **Base Classes**: `ExportDataJobBase` → `ExportDataJobAbstractTabular` → format-specific jobs
- **Format Jobs**: `ExportDataJobCsv`, `ExportDataJobExcel`, `ExportDataJobHdf5`
- **File Interfaces**: `TabularDataExportFileInterface` implemented by `DataExportXlsxFile`, etc.
- **Data Processing**: `TimestampDataMap` for tabular data assembly, `TabularDataUtility` for data manipulation
- **Excel Implementation**: Uses Apache POI with `XSSFWorkbook` for reliable XLSX generation

### Excel File Generation
The `DataExportXlsxFile` class uses `XSSFWorkbook` (non-streaming) for better reliability:
- Suitable for small to medium datasets (up to ~50K-100K rows)
- For very large files, consider switching to properly configured `SXSSFWorkbook`
- Uses proper resource management with workbook.close() in finally blocks

### Data Import Framework
The client utilities include Excel data import capabilities:
- `DataImportUtility` provides static methods for importing time-series data
- Uses Apache POI `XSSFWorkbook` for consistent Excel handling across import/export
- Supports automatic type detection (numeric → double, string → string, boolean → boolean)
- Formula evaluation supported for calculated Excel cells

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
Recent API evolution has moved from "create" to "save" semantics:
- `saveDataSet()` performs upsert operations (create or update)
- `saveAnnotation()` performs upsert operations (create or update)
- Request/Response/Result types follow `Save*Request`, `Save*Response`, `Save*Result` patterns
- Legacy "create" references should be updated to "save" when encountered

## Client API Utilities
- **Data Import**: `DataImportUtility.importXlsxData()` for importing time-series data from Excel files
  - Located in `com.ospreydcs.dp.client.utility.DataImportUtility`
  - Returns `DataImportResult` with timestamps and DataColumn objects
  - Uses Apache POI for Excel file processing
  - Expects format: `[seconds, nanos, pv_data_columns...]` with header row

## Ingestion Validation Framework
The ingestion service implements comprehensive validation for all column-oriented data structures to support high-frequency data ingestion (4000 PVs at 1 KHz) with proper memory management and data integrity.

### Validation Architecture
- **Location**: `com.ospreydcs.dp.service.ingest.handler.IngestionValidationUtility`
- **Approach**: Layered validation with fail-fast error handling
- **Error Messages**: Detailed field paths with expected vs actual values

### Validation Layers
1. **Basic Request Validation**: Provider ID, client request ID, frame presence
2. **Timestamp Validation**: SamplingClock and TimestampList validation with ordering checks
3. **Legacy Column Validation**: DataColumn and SerializedDataColumn backward compatibility
4. **New Column Validation**: All column-oriented data structures
5. **Cross-Cutting Validation**: Unique PV names across all column types

### Supported Column Types
**Scalar Columns**: DoubleColumn, FloatColumn, Int32Column, Int64Column, BoolColumn, StringColumn, EnumColumn
**Array Columns**: DoubleArrayColumn, FloatArrayColumn, Int32ArrayColumn, Int64ArrayColumn, BoolArrayColumn
**Complex Columns**: ImageColumn, StructColumn, SerializedDataColumn

### Validation Constraints
- **String Length**: 256 character maximum for StringColumn values
- **Array Dimensions**: 1-3 dimensions maximum, all dimension values > 0
- **Array Elements**: 10 million element maximum per array column
- **Image Size**: 50MB maximum per image payload  
- **Struct Size**: 1MB maximum per struct payload
- **Timestamp Integrity**: Non-decreasing timestamps, valid nanosecond ranges (0-999,999,999)
- **Sample Consistency**: All columns must have values matching timestamp count
- **Unique PV Names**: No duplicate PV names across any column type in a single frame

### Column Counting Logic
Updated `IngestionServiceImpl.ingestionResponseAck()` to count all column types:
```java
int numColumns = frame.getDataColumnsCount() + frame.getSerializedDataColumnsCount()
    + frame.getDoubleColumnsCount() + frame.getFloatColumnsCount() 
    + frame.getInt64ColumnsCount() + frame.getInt32ColumnsCount()
    + frame.getBoolColumnsCount() + frame.getStringColumnsCount()
    + frame.getEnumColumnsCount() + frame.getImageColumnsCount()
    + frame.getStructColumnsCount() + frame.getDoubleArrayColumnsCount()
    + frame.getFloatArrayColumnsCount() + frame.getInt32ArrayColumnsCount()
    + frame.getInt64ArrayColumnsCount() + frame.getBoolArrayColumnsCount();
```

## Performance Benchmarking Framework
The ingestion service includes a sophisticated benchmarking framework for performance comparison between different column-oriented data structures, particularly for high-frequency scenarios (4000 PVs at 1 KHz).

### Benchmark Architecture
- **Base Class**: `IngestionBenchmarkBase` provides common infrastructure
- **Strategy Pattern**: `ColumnBuilder` interface with implementation-specific builders
- **Factory Method**: `getColumnBuilder()` creates appropriate builder based on `ColumnDataType`
- **Threading**: Configurable multi-threaded execution with executor service pools

### Available Benchmarks
- **`BenchmarkIngestDataStream`**: Unidirectional streaming ingestion performance
- **`BenchmarkIngestDataBidiStream`**: Bidirectional streaming ingestion performance  
- **`BenchmarkIngestDataStreamBytes`**: Specialized streaming for serialized data

### Column Data Types
- **`DATA_COLUMN`**: Legacy sample-oriented DataColumn/DataValue structure (default)
- **`DOUBLE_COLUMN`**: New column-oriented DoubleColumn with packed double arrays
- **`SERIALIZED_DATA_COLUMN`**: SerializedDataColumn structure for custom serialization

### Column Builders
- **`DataColumnBuilder`**: Creates legacy DataColumn structures with individual DataValue objects per sample
- **`DoubleColumnBuilder`**: Creates efficient DoubleColumn with packed double arrays (avoids per-sample allocation)
- **`SerializedDataColumnBuilder`**: Creates SerializedDataColumn with custom serialized payload

### Usage Examples
```bash
# Run benchmark with legacy DataColumn structure (default)
java -cp target/dp-service-shaded.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestDataStream

# Run benchmark with new efficient DoubleColumn structure
java -cp target/dp-service-shaded.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestDataStream --double-column

# Run benchmark with SerializedDataColumn structure
java -cp target/dp-service-shaded.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestDataStream --serialized-column

# Display usage help
java -cp target/dp-service-shaded.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestDataStream --help
```

### Performance Comparison
The framework enables direct memory allocation and throughput comparison:
- **Legacy DataColumn**: Creates individual DataValue objects for each sample (high memory allocation)
- **New DoubleColumn**: Uses packed double arrays (minimal allocation, better cache locality)
- **Memory Impact**: At 4000 PVs × 1000 samples/sec, DataColumn creates 4M objects/sec vs DoubleColumn's 4K arrays/sec

### Benchmark Configuration
Key parameters configured in benchmark classes:
- **`numThreads`**: Executor service thread pool size (typically 7)
- **`numStreams`**: Concurrent gRPC streams (typically 20)  
- **`numRows`**: Samples per ingestion request (typically 1000)
- **`numColumns`**: PVs per stream (typically 200, total 4000 PVs)
- **`numSeconds`**: Duration of benchmark run (typically 60 seconds)

## Testing Strategy
- **Framework**: JUnit 4 (imports `org.junit.*`, uses `@Test`, `@Before`, `@After`)
- **Integration Tests**: Located in `src/test/java/com/ospreydcs/dp/service/integration/`
- **Test Base Classes**: `AnnotationTestBase`, `QueryTestBase`, `IngestionTestBase` provide common utilities
- **Test Database**: Uses "dp-test" database (cleaned between tests)
- **Scenario Methods**: Reusable test data generation (e.g., `simpleIngestionScenario()`, `createDataSetScenario()`)
- **Test Naming**: Test classes typically named `<APIMethod>Test`
- **Temporary Files**: Use `@Rule public TemporaryFolder tempFolder = new TemporaryFolder();` for test files

### Ingestion Test Framework
The ingestion test framework has been streamlined to support systematic addition of new protobuf column types with minimal boilerplate code.

**Framework Components:**
- **`IngestionTestBase.IngestionRequestParams`**: Simplified parameter object with dedicated fields for each column type
- **`buildIngestionRequest()`**: Streamlined method that uses column lists from params object
- **`GrpcIntegrationIngestionServiceWrapper.verifyIngestionRequestHandling()`**: Enhanced verification logic for all column types

**Adding New Protobuf Column Types:**
Follow this systematic 3-step process:
1. **Add Parameter Field**: Add `List<NewColumnType>` field to `IngestionTestBase.IngestionRequestParams`
2. **Update Request Builder**: Modify `buildIngestionRequest()` to include new columns in the `IngestDataRequest`
3. **Add Verification Logic**: Extend `verifyIngestionRequestHandling()` with new column type verification

**Verification Pattern:**
The verification logic follows a consistent pattern for each column type:
- Retrieve `DataColumnDocument` from stored `BucketDocument`
- Convert document to corresponding protobuf column using `toProtobufColumn()`
- Match protobuf column against original columns from the request's column list
- Verify data integrity through protobuf round-trip comparison

**Example Verification Flow:**
```java
// For DoubleColumn verification:
DataColumnDocument dataColumnDocument = bucketDocument.getDataColumnDocument();
DoubleColumn storedColumn = (DoubleColumn) dataColumnDocument.toProtobufColumn();
// Find matching column from request.getDoubleColumnsList()
// Verify storedColumn matches original request data
```

**Benefits:**
- **Systematic**: Same 3-step pattern for every new column type
- **Comprehensive**: Tests full ingestion pipeline from request → storage → retrieval
- **Maintainable**: Centralized verification logic in wrapper class
- **Extensible**: Easy to add new column types without modifying existing test infrastructure

### Ingestion Validation Test Coverage
- **Test Location**: `IngestionValidationUtilityTest` (22 test cases)
- **Legacy Validation**: Provider ID, request ID, DataColumn validation (6 tests)
- **New Column Types**: DoubleColumn, StringColumn, EnumColumn, Array, Image, Struct validation (10 tests)  
- **Advanced Scenarios**: Duplicate PV names, timestamp integrity, multi-column success cases (6 tests)
- **Error Message Testing**: Validates detailed field paths and constraint violations
- **Boundary Testing**: String length limits, array dimension limits, timestamp ordering

### Scalar Column Document Test Coverage
- **Unit Tests**: `ScalarColumnDocumentBaseTest` - Basic functionality of generic base class
- **Protobuf Conversion Tests**: `ScalarColumnDocumentBaseProtobufTest` (7 test cases)
- **Integration Tests**: `IngestDataDoubleColumnIT` - End-to-end DoubleColumn ingestion pipeline

**ScalarColumnDocumentBaseProtobufTest Coverage:**
- **Core Functionality**: Document → protobuf conversion via `toProtobufColumn()`
- **Round-trip Integrity**: Protobuf → document → protobuf data integrity
- **Legacy Compatibility**: DataColumn conversion using inherited methods
- **Serialization**: Byte array serialization through `getBytes()`
- **Edge Cases**: Empty values, null names, large datasets (1000+ values)
- **Error Handling**: Null name handling with safe fallback to empty string

**Critical for Query/Export Pipeline:**
The `toProtobufColumn()` method is essential for:
- Tabular query results (MongoDB → protobuf API responses)
- CSV export (document → protobuf → tabular assembly)
- Excel export (document → protobuf → XLSX generation)  
- HDF5 export (document → protobuf → HDF5 file creation)

**Testing Strategy Benefits:**
- **Direct Coverage**: Tests core conversion logic without full query/export complexity
- **Fast Execution**: Unit tests vs slow integration test pipelines
- **Comprehensive**: All scalar column types use same base class logic
- **Template Pattern**: Same test structure applies to all ScalarColumnDocumentBase implementations

## Continuous Integration
- **GitHub Actions**: Automated CI/CD pipeline in `.github/workflows/ci.yml`
- **Multi-Repository Setup**: Automatically builds dp-grpc dependency before testing dp-service
- **Triggers**: 
  - Automatic testing on pushes to main/master branch
  - Automatic testing on pull requests to main/master
  - Manual workflow dispatch for testing dev branches or ad-hoc validation
- **Services**: Uses MongoDB 8.0 service container for integration tests
- **Test Reports**: Uploads Surefire and Failsafe test reports as workflow artifacts
- **Dependencies**: Builds and installs dp-grpc to local Maven repository before running dp-service tests