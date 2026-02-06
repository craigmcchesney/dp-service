package com.ospreydcs.dp.service.ingest.benchmark;

import com.ospreydcs.dp.grpc.v1.common.DoubleColumn;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;

import java.util.ArrayList;
import java.util.List;

/**
 * ColumnBuilder implementation for new DoubleColumn structure.
 * 
 * This builder creates the column-oriented data structure where each column contains
 * a packed array of double values. This is the new efficient structure that avoids
 * per-sample memory allocation and should provide better performance.
 */
public class DoubleColumnBuilder implements ColumnBuilder {

    @Override
    public void buildColumns(IngestDataRequest.IngestionDataFrame.Builder frameBuilder, 
                           IngestionBenchmarkBase.IngestionTaskParams params) {
        
        final List<DoubleColumn> doubleColumnList = new ArrayList<>();
        
        for (int colIndex = params.firstColumnIndex; colIndex <= params.lastColumnIndex; colIndex++) {
            DoubleColumn.Builder columnBuilder = DoubleColumn.newBuilder();
            columnBuilder.setName(IngestionBenchmarkBase.NAME_COLUMN_BASE + colIndex);
            
            // Build packed double array directly - no DataValue wrapper objects
            for (int rowIndex = 0; rowIndex < params.numRows; rowIndex++) {
                double cellValue = rowIndex + (double) rowIndex / params.numRows;
                columnBuilder.addValues(cellValue);
            }
            
            DoubleColumn doubleColumn = columnBuilder.build();
            doubleColumnList.add(doubleColumn);
        }
        
        frameBuilder.addAllDoubleColumns(doubleColumnList);
    }

    @Override
    public String getColumnTypeName() {
        return "DoubleColumn";
    }
}