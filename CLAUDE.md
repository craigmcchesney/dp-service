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

## Testing Strategy
- **Framework**: JUnit 4 (imports `org.junit.*`, uses `@Test`, `@Before`, `@After`)
- **Integration Tests**: Located in `src/test/java/com/ospreydcs/dp/service/integration/`
- **Test Base Classes**: `AnnotationTestBase`, `QueryTestBase`, `IngestionTestBase` provide common utilities
- **Test Database**: Uses "dp-test" database (cleaned between tests)
- **Scenario Methods**: Reusable test data generation (e.g., `simpleIngestionScenario()`, `createDataSetScenario()`)
- **Test Naming**: Test classes typically named `<APIMethod>Test`
- **Temporary Files**: Use `@Rule public TemporaryFolder tempFolder = new TemporaryFolder();` for test files

### Ingestion Validation Test Coverage
- **Test Location**: `IngestionValidationUtilityTest` (22 test cases)
- **Legacy Validation**: Provider ID, request ID, DataColumn validation (6 tests)
- **New Column Types**: DoubleColumn, StringColumn, EnumColumn, Array, Image, Struct validation (10 tests)  
- **Advanced Scenarios**: Duplicate PV names, timestamp integrity, multi-column success cases (6 tests)
- **Error Message Testing**: Validates detailed field paths and constraint violations
- **Boundary Testing**: String length limits, array dimension limits, timestamp ordering

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