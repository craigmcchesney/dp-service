package com.ospreydcs.dp.service.ingest.benchmark;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;

import java.util.ArrayList;
import java.util.List;

/**
 * ColumnBuilder implementation for SerializedDataColumn structure.
 * 
 * This builder creates serialized DataColumn objects to reduce gRPC message overhead
 * while maintaining the same data semantics as DataColumn.
 */
public class SerializedDataColumnBuilder implements ColumnBuilder {

    @Override
    public void buildColumns(IngestDataRequest.IngestionDataFrame.Builder frameBuilder, 
                           IngestionBenchmarkBase.IngestionTaskParams params) {
        
        final List<SerializedDataColumn> serializedDataColumnList = new ArrayList<>();
        
        for (int colIndex = params.firstColumnIndex; colIndex <= params.lastColumnIndex; colIndex++) {
            // First build a regular DataColumn
            DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
            dataColumnBuilder.setName(IngestionBenchmarkBase.NAME_COLUMN_BASE + colIndex);
            
            // Build DataValue objects for each row in the column
            for (int rowIndex = 0; rowIndex < params.numRows; rowIndex++) {
                double cellValue = rowIndex + (double) rowIndex / params.numRows;
                DataValue dataValue = DataValue.newBuilder().setDoubleValue(cellValue).build();
                dataColumnBuilder.addDataValues(dataValue);
            }
            
            DataColumn dataColumn = dataColumnBuilder.build();
            
            // Now serialize the DataColumn into a SerializedDataColumn
            final SerializedDataColumn serializedDataColumn = SerializedDataColumn.newBuilder()
                    .setName(dataColumn.getName())
                    .setEncoding("protobuf")
                    .setPayload(dataColumn.toByteString())
                    .build();
                    
            serializedDataColumnList.add(serializedDataColumn);
        }
        
        frameBuilder.addAllSerializedDataColumns(serializedDataColumnList);
    }

    @Override
    public String getColumnTypeName() {
        return "SerializedDataColumn";
    }
}