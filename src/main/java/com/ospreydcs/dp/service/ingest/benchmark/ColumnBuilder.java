package com.ospreydcs.dp.service.ingest.benchmark;

import com.ospreydcs.dp.grpc.v1.common.DataFrame;

/**
 * Strategy interface for building different types of column data structures in ingestion benchmarks.
 * 
 * This allows the benchmark framework to support multiple column data types (DataColumn, DoubleColumn, 
 * SerializedDataColumn) using the strategy pattern for clean separation of concerns and easy extensibility.
 */
public interface ColumnBuilder {
    
    /**
     * Builds columns of the appropriate type and adds them to the IngestionDataFrame builder.
     * 
     * @param frameBuilder The IngestionDataFrame builder to add columns to
     * @param params The benchmark task parameters specifying data dimensions and properties
     */
    void buildColumns(DataFrame.Builder frameBuilder,
                      IngestionBenchmarkBase.IngestionTaskParams params);
    
    /**
     * Returns a human-readable name for this column builder type for logging and reporting.
     * 
     * @return The display name for this column builder
     */
    String getColumnTypeName();
}