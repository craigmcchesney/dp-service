package com.ospreydcs.dp.service.common.bson;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataColumnDocument {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private String name;
    private int valueCase;
    private String valueType;
    private byte[] bytes = null;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getValueCase() {
        return valueCase;
    }

    public void setValueCase(int valueCase) {
        this.valueCase = valueCase;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public void writeBytes(DataColumn dataColumn) {
        this.bytes = dataColumn.toByteArray();
    }

    public static DataColumnDocument fromDataColumn(DataColumn requestDataColumn) {
        DataColumnDocument document = new DataColumnDocument();
        document.setName(requestDataColumn.getName());
        document.writeBytes(requestDataColumn);
        if ( ! requestDataColumn.getDataValuesList().isEmpty()) {
            final DataValue.ValueCase dataValueCase = requestDataColumn.getDataValues(0).getValueCase();
            document.setValueCase(dataValueCase.getNumber());
            document.setValueType(dataValueCase.name());
        }
        return document;
    }

    public static DataColumnDocument fromSerializedDataColumn(
            SerializedDataColumn column
    ) {
        DataColumnDocument document = new DataColumnDocument();
        document.setName(column.getName());
        document.setBytes(column.getPayload().toByteArray());
        return document;
    }

    public DataColumn toDataColumn() throws DpException {

        final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();

        if (this.bytes != null) {
            try {
                return DataColumn.parseFrom(this.bytes);
            } catch (InvalidProtocolBufferException e) {
                final String errorMsg =
                        "DataColumnDocument.toDataColumn() error parsing serialized byte array: " + e.getMessage();
                logger.error(errorMsg);
                throw new DpException(errorMsg);
            }
        }

        return dataColumnBuilder.build();
    }

    public SerializedDataColumn toSerializedDataColumn() throws DpException {
        final SerializedDataColumn.Builder serializedDataColumnBuilder = SerializedDataColumn.newBuilder();
        if (this.bytes != null) {
            serializedDataColumnBuilder.setPayload(ByteString.copyFrom(this.getBytes()));
        }
        serializedDataColumnBuilder.setName(this.getName());
        return serializedDataColumnBuilder.build();
    }
}
