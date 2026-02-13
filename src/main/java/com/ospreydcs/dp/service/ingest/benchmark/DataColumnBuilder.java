package com.ospreydcs.dp.service.ingest.benchmark;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataFrame;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;

import java.util.ArrayList;
import java.util.List;

/**
 * ColumnBuilder implementation for legacy DataColumn/DataValue structure.
 * 
 * This builder creates the sample-oriented data structure where each column contains
 * a list of DataValue objects. This is the original API structure that will be deprecated
 * but is maintained for performance comparison benchmarks.
 */
public class DataColumnBuilder implements ColumnBuilder {

    @Override
    public void buildColumns(
            DataFrame.Builder frameBuilder,
            IngestionBenchmarkBase.IngestionTaskParams params
    ) {
        final List<DataColumn> dataColumnList = new ArrayList<>();
        
        for (int colIndex = params.firstColumnIndex; colIndex <= params.lastColumnIndex; colIndex++) {
            DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
            dataColumnBuilder.setName(IngestionBenchmarkBase.NAME_COLUMN_BASE + colIndex);
            
            // Build DataValue objects for each row in the column
            for (int rowIndex = 0; rowIndex < params.numRows; rowIndex++) {
                double cellValue = rowIndex + (double) rowIndex / params.numRows;
                DataValue dataValue = DataValue.newBuilder().setDoubleValue(cellValue).build();
                dataColumnBuilder.addDataValues(dataValue);
            }
            
            DataColumn dataColumn = dataColumnBuilder.build();
            dataColumnList.add(dataColumn);
        }
        
        frameBuilder.addAllDataColumns(dataColumnList);
    }

    @Override
    public String getColumnTypeName() {
        return "DataColumn";
    }
}