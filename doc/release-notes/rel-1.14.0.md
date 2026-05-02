# dp-service 1.14.0 Release Notes

## Column-Level Metadata in Ingestion (Issues dp-grpc #116, dp-service #177)

### gRPC API Changes (dp-grpc #116)

Two new protobuf messages were added to `common.proto`:

```protobuf
message ColumnProvenance {
  string source = 1;   // origin system (e.g. "archiver", "epics-bridge")
  string process = 2;  // originating process or pipeline step
}

message ColumnMetadata {
  ColumnProvenance provenance = 1;
  repeated string tags = 2;
  repeated Attribute attributes = 3;
}
```

An optional `metadata` field (field number 10) was added to all 16 column message types: `DataColumn`, `SerializedDataColumn`, `DoubleColumn`, `FloatColumn`, `Int64Column`, `Int32Column`, `BoolColumn`, `StringColumn`, `EnumColumn`, `DoubleArrayColumn`, `FloatArrayColumn`, `Int32ArrayColumn`, `Int64ArrayColumn`, `BoolArrayColumn`, `StructColumn`, and `ImageColumn`.

### Ingestion Service Changes (dp-service #177)

**Metadata persistence:** When an `IngestDataRequest` includes `ColumnMetadata` on any column, the metadata is now extracted and stored as a `columnMetadata` subdocument on the column's MongoDB bucket document. The field is stored within `ColumnDocumentBase` (the base class for all column document types), making it available under the dot-notation path `dataColumn.columnMetadata` for future query support. All 16 column types are handled.

**New BSON document classes:**
- `ColumnProvenanceDocument` — persists `source` and `process` fields
- `ColumnMetadataDocument` — persists `provenance`, `tags`, and `attributes` (attributes stored as a sorted `Map<String, String>` via `AttributesUtility`)

**Metadata validation:** A new validation layer was added to `IngestionValidationUtility` covering all column types. Limits enforced:
- `provenance.source` and `provenance.process`: ≤ 256 characters each
- `tags`: ≤ 20 entries, each ≤ 256 characters
- `attributes`: ≤ 20 entries, each key and value ≤ 256 characters

Columns without a `metadata` field are unaffected and incur no additional overhead.

**Proto round-trip:** `toProtobufColumn()` was updated across all column document types to restore the `ColumnMetadata` field when deserializing from MongoDB, ensuring the retrieved proto column equals the original ingested column.

**Test coverage:** 26 unit tests (BSON document classes and factory methods), 9 validation unit tests, and 5 end-to-end integration tests covering full metadata, absent metadata, partial metadata (provenance only), mixed columns (one with metadata, one without), and all three column categories (scalar, array, binary).

## PV Metadata API and Query Service Renames (Issues dp-grpc #117, dp-service #178)

### gRPC API Changes (dp-grpc #117)

**Query service renames:** `queryPvMetadata()` and `queryProviderMetadata()` were renamed to `queryPvStats()` and `queryProviderStats()` to better reflect that these methods return archive ingestion statistics rather than user-defined metadata. All associated request, response, and result message types were renamed accordingly:

| Old name | New name |
|---|---|
| `QueryPvMetadataRequest/Response` | `QueryPvStatsRequest/Response` |
| `QueryProviderMetadataRequest/Response` | `QueryProviderStatsRequest/Response` |
| `MetadataResult` (nested) | `StatsResult` |
| `PvInfo` (nested) | `PvStats` |
| `ProviderMetadata` (standalone message) | `ProviderStats` |

**New PV Metadata API on `DpAnnotationService`:** Six new RPC methods for managing a `pvMetadata` collection:
- `savePvMetadata(SavePvMetadataRequest)` — create or update a PV metadata record
- `queryPvMetadata(QueryPvMetadataRequest)` — search records by name, alias, tags, or attributes
- `getPvMetadata(GetPvMetadataRequest)` — retrieve a single record by canonical name or alias
- `deletePvMetadata(DeletePvMetadataRequest)` — remove a record by canonical name or alias
- `patchPvMetadata(PatchPvMetadataRequest)` — stub, not yet implemented
- `bulkSavePvMetadata(BulkSavePvMetadataRequest)` — stub, not yet implemented

The `PvMetadata` message is defined in `common.proto` and carries `pvName`, `aliases`, `tags`, `attributes`, `description`, `modifiedBy`, `createdTime`, and `updatedTime`.

### Query Service Changes (dp-service #178)

Pure nomenclature rename across the query service layer — no behavioral changes. All classes, methods, and message type references in `QueryServiceImpl`, `QueryHandlerInterface`, `MongoQueryHandler`, `MongoQueryClientInterface`, `MongoSyncQueryClient`, dispatcher classes, and job classes were updated to reflect the new `Stats` naming.

### Annotation Service Changes (dp-service #178)

**New `pvMetadata` MongoDB collection:** A new collection stores `PvMetadataDocument` records with the following fields: `pvName` (canonical name, unique index), `aliases` (regular index), `tags`, `attributes`, `description`, `modifiedBy`, `createdAt`, and `updatedAt`. Indexes are created at startup.

**`savePvMetadata`:** Upserts by `pvName`. On create, sets `createdAt`; on update, preserves `createdAt` and sets `updatedAt`. Tags are normalized to a lowercase, deduplicated, sorted list. Aliases are enforced to be globally unique across all PV records — a conflict with a different `pvName` is rejected.

**`queryPvMetadata`:** Builds a MongoDB filter from a list of criteria combined with AND semantics. Each criterion is one of:
- `PvNameCriterion` — exact, prefix, or contains match on `pvName`
- `AliasesCriterion` — exact, prefix, or contains match on `aliases`
- `TagsCriterion` — `$in` match on `tags`
- `AttributesCriterion` — key-only (`$exists`) or key + values (`$in`) match on `attributes`

Results are sorted by `pvName` ascending. Pagination uses a base64-encoded integer skip offset passed as `pageToken`.

**`getPvMetadata` / `deletePvMetadata`:** Look up by canonical `pvName` or alias. Return `RESULT_STATUS_REJECT` if no record is found.

**Validation:**
- `savePvMetadata`: `pvName` not blank; no duplicate attribute keys; no alias conflicts with other records
- `queryPvMetadata`: criteria list non-empty; `AttributesCriterion.key` not blank
- `getPvMetadata` / `deletePvMetadata`: `pvNameOrAlias` not blank

**Test coverage:** 27 integration tests in `PvMetadataIT` covering all CRUD operations, all query criterion types (exact, prefix, contains, tags, attribute key-only and key+value, multi-criterion AND), pagination, alias conflict detection, not-found handling, and stub method responses.

## Machine Configuration API (Issues dp-grpc #118, dp-service #181)

### gRPC API Changes (dp-grpc #118)

**New `Configuration` and `ConfigurationActivation` messages** added to `common.proto`:

- `Configuration` — represents a named machine configuration record with fields: `configurationName`, `category`, `description`, `modifiedBy`, `tags`, `attributes`, `createdTime`, `updatedTime`
- `ConfigurationActivation` — represents a time-bounded activation of a configuration with fields: `clientActivationId`, `configurationName`, `startTime`, `endTime` (optional; absent = open-ended), `description`, `modifiedBy`, `tags`, `attributes`, `createdTime`, `updatedTime`

**New RPC methods on `DpAnnotationService`:**

*Configuration management:*
- `saveConfiguration(SaveConfigurationRequest)` — create or update a configuration record
- `queryConfigurations(QueryConfigurationsRequest)` — search records by name, category, tags, or attributes
- `getConfiguration(GetConfigurationRequest)` — retrieve a single record by `configurationName`
- `deleteConfiguration(DeleteConfigurationRequest)` — remove a record by `configurationName`
- `patchConfiguration(PatchConfigurationRequest)` — stub, not yet implemented
- `bulkSaveConfiguration(BulkSaveConfigurationRequest)` — stub, not yet implemented

*ConfigurationActivation management:*
- `saveConfigurationActivation(SaveConfigurationActivationRequest)` — create or update an activation record; rejects overlapping activations for the same configuration name or category
- `getConfigurationActivation(GetConfigurationActivationRequest)` — retrieve a single activation by `clientActivationId` or by composite key (`configurationName` + `startTime`)
- `queryConfigurationActivations(QueryConfigurationActivationsRequest)` — search activations by timestamp, time range, configuration name, client activation ID, category, tags, or attributes
- `deleteConfigurationActivation(DeleteConfigurationActivationRequest)` — remove an activation by `clientActivationId` or by composite key
- `getActiveConfigurations(GetActiveConfigurationsRequest)` — return all activations whose time interval contains a given timestamp (or the current time if omitted)
- `patchConfigurationActivation(PatchConfigurationActivationRequest)` — stub, not yet implemented
- `bulkSaveConfigurationActivation(BulkSaveConfigurationActivationRequest)` — stub, not yet implemented

### Annotation Service Changes (dp-service #181)

**New `configurations` MongoDB collection:** Stores `ConfigurationDocument` records with fields: `configurationName` (unique index), `category` (index), `description`, `modifiedBy`, `tags`, `attributes`, `createdAt`, `updatedAt`.

**New `configurationActivations` MongoDB collection:** Stores `ConfigurationActivationDocument` records with fields: `clientActivationId` (unique sparse index), `configurationName` (index), `internalCategory` (denormalized from `Configuration.category` at save time; index), `startTime` (index), `endTime` (index), `description`, `modifiedBy`, `tags`, `attributes`, `createdAt`, `updatedAt`.

**`saveConfiguration`:** Upserts by `configurationName`. On create, sets `createdAt`; on update, preserves `createdAt` and sets `updatedAt`. Tags are normalized to a lowercase, deduplicated, sorted list. Category changes are rejected if any activations exist for the configuration.

**`queryConfigurations`:** Builds a MongoDB filter from a list of criteria combined with AND semantics. Each criterion is one of:
- `ConfigurationNameCriterion` — exact, prefix, or contains match on `configurationName`
- `CategoryCriterion` — exact, prefix, or contains match on `category`
- `TagsCriterion` — `$in` match on `tags`
- `AttributesCriterion` — key-only (`$exists`) or key + values (`$in`) match on `attributes`

Results are sorted by `configurationName` ascending. Pagination uses a base64-encoded integer skip offset passed as `pageToken`.

**`getConfiguration` / `deleteConfiguration`:** Look up by `configurationName`. Return `RESULT_STATUS_REJECT` if no record is found. `deleteConfiguration` additionally rejects if any activations exist for the configuration.

**`saveConfigurationActivation`:** Upserts by `clientActivationId` (a server-generated UUID is assigned if not supplied by the client). Looks up the referenced `Configuration` to denormalize `internalCategory`. Enforces an overlap constraint: rejects the save if any existing activation for the same `configurationName` or the same `internalCategory` has a time interval that overlaps the new activation's interval. On create, sets `createdAt`; on update, preserves `createdAt`, sets `updatedAt`, and excludes the record being updated from the overlap check.

**`getConfigurationActivation`:** Retrieves by `clientActivationId` or by composite key (`configurationName` + `startTime`). Returns `RESULT_STATUS_REJECT` if not found.

**`queryConfigurationActivations`:** Builds a MongoDB filter from a list of criteria combined with AND semantics. Each criterion is one of:
- `TimestampCriterion` — activations whose interval contains the given timestamp
- `TimeRangeCriterion` — activations whose interval overlaps the given time range
- `ConfigurationNameCriterion` — `$in` match on `configurationName`
- `ClientActivationIdCriterion` — `$in` match on `clientActivationId`
- `CategoryCriterion` — `$in` match on `internalCategory`
- `TagsCriterion` — `$in` match on `tags`
- `AttributesCriterion` — key-only or key + values match on `attributes`

Results are sorted by `configurationName` then `startTime` ascending. Pagination uses a base64-encoded integer skip offset.

**`deleteConfigurationActivation`:** Removes by `clientActivationId` or by composite key. Returns `RESULT_STATUS_REJECT` if not found.

**`getActiveConfigurations`:** Returns all activations whose `startTime ≤ timestamp` and (`endTime > timestamp` or `endTime` is absent). If no timestamp is specified in the request, the current server time is used.

**Validation:**
- `saveConfiguration`: `configurationName` not blank; `category` not blank; no duplicate attribute keys; category changes blocked if activations exist
- `queryConfigurations`: criteria list non-empty; `AttributesCriterion.key` not blank
- `getConfiguration` / `deleteConfiguration`: `configurationName` not blank
- `saveConfigurationActivation`: `configurationName` not blank; `startTime` specified; `endTime` (if present) after `startTime`; no overlapping activation for same config name or category
- `queryConfigurationActivations`: criteria list non-empty; each criterion fully specified
- `getConfigurationActivation` / `deleteConfigurationActivation`: lookup key not blank
- `getActiveConfigurations`: no required fields (defaults to current time)

**Test coverage:** 69 integration tests in `ConfigurationIT` covering all CRUD operations for both `Configuration` and `ConfigurationActivation`, all query criterion types, pagination, overlap rejection, open-ended activations, category-change blocking, composite-key lookups, not-found handling, `getActiveConfigurations` with and without a timestamp, and stub method responses.
