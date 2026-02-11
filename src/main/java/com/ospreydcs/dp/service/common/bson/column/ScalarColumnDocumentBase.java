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

    protected abstract DataValue createDataValueFromScalar(T value);

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