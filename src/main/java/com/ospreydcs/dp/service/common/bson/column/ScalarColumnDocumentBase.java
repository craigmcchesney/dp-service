package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.util.List;

@BsonDiscriminator
public abstract class ScalarColumnDocumentBase<T> extends ColumnDocumentBase {

    private List<T> values;

    public List<T> getValues() {
        return values;
    }

    public void setValues(List<T> values) {
        this.values = values;
    }

    protected abstract Message.Builder createColumnBuilder();
    
    protected abstract void addAllValuesToBuilder(Message.Builder builder, List<T> values);
    
    protected abstract DataValue createDataValueFromScalar(T value);

    protected Message toProtobufColumn() {
        Message.Builder builder = createColumnBuilder();
        setBuilderName(builder, this.getName());
        addAllValuesToBuilder(builder, this.getValues());
        return builder.build();
    }

    private void setBuilderName(Message.Builder builder, String name) {
        try {
            // Handle null names by using empty string
            String safeName = (name != null) ? name : "";
            builder.getClass().getMethod("setName", String.class).invoke(builder, safeName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set name on column builder", e);
        }
    }

    @Override
    public byte[] getBytes() {
        return toProtobufColumn().toByteArray();
    }

    @Override
    public DataColumn toDataColumn() throws DpException {
        DataColumn.Builder builder = DataColumn.newBuilder();
        builder.setName(this.getName());
        
        for (T value : this.getValues()) {
            DataValue dataValue = createDataValueFromScalar(value);
            builder.addDataValues(dataValue);
        }
        return builder.build();
    }
}