package com.ospreydcs.dp.service.ingest.benchmark;

/**
 * Enum to specify the type of column data structure to use in ingestion benchmarks.
 * This allows performance comparison between different column-oriented data structures.
 */
public enum ColumnDataType {
    /**
     * Legacy DataColumn/DataValue structure - sample-oriented with individual DataValue objects.
     * This is the original API structure that will be deprecated.
     */
    DATA_COLUMN,

    /**
     * New DoubleColumn structure - column-oriented with packed double array.
     * This is the new efficient structure for double-precision data.
     */
    DOUBLE_COLUMN,

    /**
     * SerializedDataColumn structure - serialized DataColumn for network efficiency.
     * This serializes DataColumn objects to reduce gRPC message overhead.
     */
    SERIALIZED_DATA_COLUMN
}