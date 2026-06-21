## 0. Cloning Repositories and Building

### Environment Assumptions

These instructions assume that Maven and Java are installed on the host where the tests will be run, and that MongoDB is installed and configured for use by the tests.

### Cloning and Building the dp-grpc Repo

The prerequisite for running the tests from the dp-service repo (in addition to a running MongoDB installation) is to clone and build the dp-grpc project repo.  Here are the steps:

1. Clone the dp-grpc repo:
```bash
git clone https://github.com/osprey-dcs/dp-grpc.git
```
2. Change to the dp-grpc directory and run `mvn install`:
```bash
mvn install
```

### Cloning and Building the dp-service Repo

The tests themselves are contained in the dp-service repo.  After building and installing the dp-grpc repo, clone and build the dp-service repo:

1. Clone the dp-service repo:
```bash
git clone https://github.com/osprey-dcs/dp-service.git
```
2. Change to the dp-service directory and run `mvn compile`:
```bash
mvn compile
```



## 1. Overriding Database Settings and Running Tests

The dp-service regression tests default to the URI `mongodb://admin:admin@localhost:27017/` and the database name `dp-test`.  You can override either or both of those settings as described below.

**WARNING: the database with the specified name WILL BE DROPPED before executing each test — DO NOT USE A DATABASE THAT CONTAINS DATA YOU WISH TO KEEP.**

Here is an example for overriding both settings via environment variables and running all the tests in `DoubleColumnIT.java`:

```bash
export DP_MONGO_DB_URI="mongodb://admin:admin@localhost:27017/"
export DP_MONGO_TEST_DB_NAME="dp-test-override"
mvn test -Dtest=DoubleColumnIT
```

Or combine the environment variables and Maven command on a single line (which avoids polluting the shell environment):

```bash
DP_MONGO_DB_URI="mongodb://admin:admin@localhost:27017/" DP_MONGO_TEST_DB_NAME="dp-test-override" mvn test -Dtest=DoubleColumnIT
```

An individual method from a test class can be run by specifying it on the command line using a `#` separator between the class and method names:

```bash
DP_MONGO_DB_URI="mongodb://admin:admin@localhost:27017/" DP_MONGO_TEST_DB_NAME="dp-test-override" mvn test -Dtest=IngestDataColumnMetadataIT#testIngestDoubleColumnWithFullMetadata
```



## 2. Column-Oriented Ingestion Tests

There is a JUnit test class for each of the new gRPC data column message types (e.g., `DoubleColumnIT` in `src/test/integration/java/com/ospreydcs/dp/service/integration/v2api/`).  Each class tests ingesting data of the specified column type and exercises downstream features like `queryData()`, `subscribeData()`, and export, verifying that responses are correct for the given data type.  After running a test you can inspect the MongoDB `buckets` collection to confirm the ingested data looks as expected.

To run the test for a specific column type, e.g., `DoubleColumnIT`:

```bash
DP_MONGO_DB_URI="mongodb://admin:admin@localhost:27017/" DP_MONGO_TEST_DB_NAME="dp-test-override" mvn test -Dtest=DoubleColumnIT
```

After running this test, the `buckets` collection will contain 2 documents.  The key thing to verify is that the `dataColumn` field contains the appropriate type — for `DoubleColumnIT` the type (`_t`) will be `doubleColumn`, and the individual values will be visible in the `dataColumn.values` field.  The same naming pattern applies to all other column types.

There are additional core ingestion framework tests covering unary vs. streaming, rejections, etc., but the column-oriented tests are the recommended starting point.



## 3. Bucket-Level Ingestion Metadata Tests

The test coverage for bucket-level ingestion metadata is in `src/test/integration/java/com/ospreydcs/dp/service/integration/v2api/IngestDataColumnMetadataIT.java`.  Unlike the column ingestion tests, this class contains multiple test methods, each with a comment describing its coverage.

To run an individual method:

```bash
DP_MONGO_DB_URI="mongodb://admin:admin@localhost:27017/" DP_MONGO_TEST_DB_NAME="dp-test-override" mvn test -Dtest=IngestDataColumnMetadataIT#testIngestDoubleColumnWithFullMetadata
```

The key method to verify is `testIngestDoubleColumnWithFullMetadata`.  The others can be run as desired to test other parameter combinations or column types.

### testIngestDoubleColumnWithFullMetadata

Tests that full metadata (provenance + tags + attributes) is persisted.  Creates a single `BucketDocument` in the `buckets` collection.  The `dataColumn.columnMetadata` field contains the metadata document with attributes, provenance, tags, and name as provided in the request, in addition to the data values.

### Other Column Metadata Tests

- `testIngestDoubleColumnWithoutMetadata` — Ingests a column without metadata and checks that `columnMetadata` is null.
- `testIngestDoubleColumnWithProvenanceOnly` — Tests with provenance metadata only; checks that tags and attributes are null.
- `testIngestMultipleColumnsOnlyOneWithMetadata` — Tests ingestion of two columns, one with metadata and one without.
- `testIngestAllColumnCategoriesWithMetadata` — Tests 3 columns with metadata: one scalar, one array, one binary.



## 4. PV Metadata Tests

The JUnit test class for the PV metadata API is `src/test/integration/java/com/ospreydcs/dp/service/integration/annotation/PvMetadataIT.java`.  It contains many positive (success) and negative (failure) test methods covering the full API.  Method names give a clue about what each covers.

### testSavePvMetadataCreateAndUpdate

Tests saving and updating a `PvMetadataDocument`.  After running, the database contains a single document in the `pvMetadata` collection with aliases, attributes, description, modifier, PV name, and tags as specified in the test.

### Save Reject Tests

- `testSavePvMetadataRejectPvNameIsAliasOfOther` — Attempts to save a record whose `pvName` equals the alias of an existing record; confirms rejection.
- `testSavePvMetadataRejectAliasConflict` — Attempts to use an alias already assigned to a different PV name; confirms rejection.

### queryPvMetadata Reject Tests

- `testQueryPvMetadataRejectEmptyCriteria` — Sends a query with empty criteria.
- `testQueryPvMetadataRejectBlankAttributeKey` — Sends attribute query criteria with a blank attribute key.

### Positive queryPvMetadata Tests

- `testQueryPvMetadataEmptyResult` — Sends a query that returns no data; checks result is empty.
- `testQueryPvMetadataByPvNameExact` — Tests query using exact PV name match.
- `testQueryPvMetadataByPvNamePrefix` — Tests query using PV name prefix.
- `testQueryPvMetadataByPvNameContains` — Tests query using PV name substring.
- `testQueryPvMetadataByAliasExact` — Tests query by exact alias match.
- `testQueryPvMetadataByAliasContains` — Tests query by alias substring.
- `testQueryPvMetadataByTags` — Tests query by tags.
- `testQueryPvMetadataByAttributeKeyOnly` — Tests query by attribute key.
- `testQueryPvMetadataByAttributeKeyAndValue` — Tests query by attribute key and value.
- `testQueryPvMetadataMultiCriterionAnd` — Tests query with multiple criteria.
- `testQueryPvMetadataPagination` — Tests query pagination.

### getPvMetadata Reject Tests

- `testGetPvMetadataRejectBlankPvNameOrAlias` — Sends get without a PV name or alias.
- `testGetPvMetadataNotFound` — Sends get for a non-existent name or alias.

### Positive getPvMetadata Tests

- `testGetPvMetadataByPvName` — Tests successful `getPvMetadata` by PV name.
- `testGetPvMetadataByAlias` — Tests successful `getPvMetadata` by alias.

### Delete Reject Tests

- `testDeletePvMetadataRejectBlankPvNameOrAlias` — Tests delete with a blank PV name.
- `testDeletePvMetadataNotFound` — Tests delete for a non-existent PV name.

### Positive Delete Tests

- `testDeletePvMetadataByPvName` — Tests successful delete by PV name.
- `testDeletePvMetadataByAlias` — Tests successful delete by alias.



## 5. Machine Configuration API Tests

The JUnit test class for the machine configuration API is `src/test/integration/java/com/ospreydcs/dp/service/integration/annotation/ConfigurationIT.java`.  It contains tests for both configuration definition and configuration activation.  This section covers the configuration definition methods; the following section covers activation.

### testSaveConfigurationCreateAndUpdate

The key positive test for defining configurations.  After running, the `configurations` collection contains a single document with tags, attributes, description, category, `configurationName`, and modifier set as specified in the create and update requests, with tags normalized to lowercase unique sorted order.

### testSaveConfigurationWithParent

Tests saving a child configuration that references a parent configuration name.  After running, the `configurations` collection contains two documents — one for the parent and one for the child — with the child containing a link in the `parentConfigurationName` field.

### Save Reject Tests

- `testSaveConfigurationRejectBlankName` — Attempts to save a configuration with a blank name; confirms rejection.
- `testSaveConfigurationRejectBlankCategory` — Attempts to save a configuration with a blank category; confirms rejection.
- `testSaveConfigurationRejectDuplicateAttributeKeys` — Attempts to save a configuration with duplicate attribute keys; confirms rejection.

### Positive getConfiguration Tests

- `testGetConfigurationSuccess` — Saves a configuration then retrieves it by name; verifies name, category, and description in the response.

### getConfiguration Reject Tests

- `testGetConfigurationRejectBlankName` — Sends get with a blank configuration name; confirms rejection.
- `testGetConfigurationRejectNotFound` — Sends get for a non-existent configuration name; confirms rejection.

### queryConfigurations Reject Tests

- `testQueryConfigurationsRejectEmptyCriteria` — Sends a query with an empty criteria list; confirms rejection.

### Positive queryConfigurations Tests

- `testQueryConfigurationsByNameExact` — Tests query using exact name match; verifies exactly one result returned.
- `testQueryConfigurationsByNamePrefix` — Tests query using name prefix; verifies the two matching records are returned.
- `testQueryConfigurationsByNameContains` — Tests query using name substring; verifies three matching records are returned.
- `testQueryConfigurationsByCategory` — Tests query by category value; verifies all results have the specified category.
- `testQueryConfigurationsByParent` — Tests query by parent configuration name; verifies the child configuration is returned.
- `testQueryConfigurationsByTags` — Tests query by tag value; verifies records with the specified tag are returned.
- `testQueryConfigurationsByAttributeKeyOnly` — Tests query by attribute key with no value; verifies only records with that attribute key are returned.
- `testQueryConfigurationsByAttributeKeyAndValue` — Tests query by attribute key and value; verifies only matching records are returned.
- `testQueryConfigurationsEmptyResult` — Tests query that matches no records; verifies an empty list is returned without error.
- `testQueryConfigurationsPagination` — Tests paginated query with limit=3 across 5 records; verifies first page returns 3 records with a `nextPageToken`, and second page returns the remaining 2.

### Positive deleteConfiguration Tests

- `testDeleteConfigurationSuccess` — Saves then deletes a configuration; verifies the deleted name is returned.

### deleteConfiguration Reject Tests

- `testDeleteConfigurationRejectBlankName` — Tests delete with a blank configuration name; confirms rejection.
- `testDeleteConfigurationRejectNotFound` — Tests delete for a non-existent configuration; confirms rejection.
- `testDeleteConfigurationDoubleDeleteRejected` — Saves, deletes successfully, then attempts a second delete; confirms rejection.



## 6. Machine Configuration Activation API Tests

The JUnit test class for machine configuration activation is also `ConfigurationIT.java`.  This section covers the configuration activation methods; the previous section covers configuration definition.

### testSaveConfigurationActivationClientIdPreserved

Key positive test for activating a configuration.  Saves an activation with a client-supplied `clientActivationId` and verifies the ID is preserved in the response and in MongoDB.  Also verifies that `configurationName` and `internalCategory` (denormalized from the configuration's category) are stored correctly.  After running, the `configurationActivations` collection contains a single document with the `clientActivationId`, `configurationName`, `category`, and `startTime` specified in the request.

### testSaveConfigurationActivationCreateAndUpdate

Tests upsert semantics for an activation record.  Creates an activation, verifies `createdAt` is set and `updatedAt` is null, then updates the same record and verifies that `createdAt` is preserved and `updatedAt` is set.  After running, the collection contains a single document with `clientActivationId`, `configurationName`, `description`, `startTime`, `endTime`, and `category` set from the request.

### testSaveConfigurationActivationServerGeneratesId

Tests that when no `clientActivationId` is provided, the server generates a UUID.  Verifies the returned ID is a 36-character UUID string.  After running, the collection contains a single document with the generated `clientActivationId`, and `configurationName`, `internalCategory`, and `startTime` set in the request.

### testSaveConfigurationActivationOpenEnded

Tests saving an activation with no `endTime`.  Verifies the record is saved successfully and that `endTime` is null in MongoDB.

### saveConfigurationActivation Reject Tests

- `testSaveConfigurationActivationRejectBlankConfigName` — Attempts to save an activation with a blank configuration name; confirms rejection.
- `testSaveConfigurationActivationRejectMissingStartTime` — Attempts to save an activation without a start time; confirms rejection.
- `testSaveConfigurationActivationRejectEndTimeBeforeStartTime` — Attempts to save an activation where `endTime` is before `startTime`; confirms rejection.
- `testSaveConfigurationActivationRejectDuplicateAttributeKeys` — Attempts to save an activation with duplicate attribute keys; confirms rejection.
- `testSaveConfigurationActivationRejectOverlapSameConfigName` — Saves an activation for t0–t2, then attempts to save a second overlapping activation (t1–t3) for the same configuration; confirms rejection.
- `testSaveConfigurationActivationRejectOverlapSameCategory` — Saves an activation for configuration A (t0–t2), then attempts to save an overlapping activation (t1–t3) for a different configuration in the same category; confirms rejection.
- `testSaveConfigurationActivationRejectOverlapWithOpenEnded` — Saves an open-ended activation, then attempts to save another activation starting after it for the same configuration; confirms rejection.

### Positive getConfigurationActivation Tests

- `testGetConfigurationActivationByIdSuccess` — Retrieves an activation by `clientActivationId`; verifies ID and configuration name in the response.
- `testGetConfigurationActivationByCompositeKeySuccess` — Retrieves an activation by composite key (`configurationName` + `startTime`); verifies configuration name in the response.

### getConfigurationActivation Reject Tests

- `testGetConfigurationActivationByIdRejectBlankId` — Sends get with a blank activation ID; confirms rejection.
- `testGetConfigurationActivationByCompositeKeyRejectBlankName` — Sends get with a blank configuration name in the composite key; confirms rejection.
- `testGetConfigurationActivationByCompositeKeyRejectMissingStartTime` — Sends get with a zero-value start time in the composite key; confirms rejection.
- `testGetConfigurationActivationByIdRejectNotFound` — Sends get for a non-existent activation ID; confirms rejection.
- `testGetConfigurationActivationByCompositeKeyRejectNotFound` — Sends get for a non-existent composite key; confirms rejection.

### queryConfigurationActivations Reject Tests

- `testQueryConfigurationActivationsRejectEmptyCriteria` — Sends a query with an empty criteria list; confirms rejection.
- `testQueryConfigurationActivationsRejectMissingTimestamp` — Sends a timestamp criterion with no timestamp value; confirms rejection.
- `testQueryConfigurationActivationsRejectMissingTimeRangeStart` — Sends a time range criterion with no start time; confirms rejection.
- `testQueryConfigurationActivationsRejectEmptyConfigNameValues` — Sends a configuration name criterion with no values; confirms rejection.

### Positive queryConfigurationActivations Tests

- `testQueryConfigurationActivationsByTimestamp` — Queries at a specific timestamp within the t0–t1 range; verifies the single overlapping activation is returned.
- `testQueryConfigurationActivationsByTimeRange` — Queries for the t0–t2 time range; verifies the two activations within that range are returned.
- `testQueryConfigurationActivationsByConfigurationName` — Queries by configuration name; verifies the matching activation is returned.
- `testQueryConfigurationActivationsByClientActivationId` — Queries by client activation ID; verifies the exact matching activation is returned.
- `testQueryConfigurationActivationsByCategory` — Queries by category; verifies the two activations in that category are returned.
- `testQueryConfigurationActivationsByTags` — Queries by tag value; verifies activations with the specified tag are returned.
- `testQueryConfigurationActivationsByAttributeKeyOnly` — Queries by attribute key only; verifies activations that have the attribute key are returned.
- `testQueryConfigurationActivationsByAttributeKeyAndValue` — Queries by attribute key and value; verifies only the exact matching activation is returned.
- `testQueryConfigurationActivationsEmptyResult` — Queries for a non-existent activation ID; verifies an empty list is returned without error.

### Positive deleteConfigurationActivation Tests

- `testDeleteConfigurationActivationByIdSuccess` — Saves then deletes an activation by `clientActivationId`; verifies the deleted ID is returned.
- `testDeleteConfigurationActivationByCompositeKeySuccess` — Saves then deletes an activation by composite key; verifies the actual `clientActivationId` (not a synthetic composite-key string) is returned.

### deleteConfigurationActivation Reject Tests

- `testDeleteConfigurationActivationByIdRejectBlankId` — Tests delete with a blank activation ID; confirms rejection.
- `testDeleteConfigurationActivationByIdRejectNotFound` — Tests delete for a non-existent activation ID; confirms rejection.
- `testDeleteConfigurationActivationByCompositeKeyRejectNotFound` — Tests delete for a non-existent composite key; confirms rejection.
- `testDeleteConfigurationActivationDoubleDeleteRejected` — Saves, deletes successfully, then attempts a second delete; confirms rejection.

### Positive getActiveConfigurations Tests

- `testGetActiveConfigurationsDefaultsToNow` — Saves an open-ended activation starting in the past and calls `getActiveConfigurations` without a timestamp; verifies the server defaults to the current time and returns the open-ended record.
- `testGetActiveConfigurationsAtT0Plus1` — Queries at t0+1s; verifies only the activation covering t0–t2 is returned.
- `testGetActiveConfigurationsAtT1Plus1` — Queries at t1+1s; verifies the two activations covering that time (t0–t2 and t1–t3) are both returned.
- `testGetActiveConfigurationsAtT2Plus1` — Queries at t2+1s; verifies the two activations covering that time (t1–t3 and t2–t4) are returned.
- `testGetActiveConfigurationsAtT3Plus1` — Queries at t3+1s; verifies the bounded activation (t2–t4) and the open-ended activation (t3–open) are both returned.
- `testGetActiveConfigurationsAtT5` — Queries at t5 (after t4); verifies only the open-ended activation (t3–open) remains active.
- `testGetActiveConfigurationsNoneActive` — Queries before any activations start; verifies an empty result is returned without error.
