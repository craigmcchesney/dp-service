package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.DoubleColumn;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.util.List;

@BsonDiscriminator(key = "_t", value = "doubleColumn")
public class DoubleColumnDocument extends ScalarColumnDocumentBase<Double> {

    public static DoubleColumnDocument fromDoubleColumn(DoubleColumn requestColumn) {
        DoubleColumnDocument document = new DoubleColumnDocument();
        document.setName(requestColumn.getName());
        document.setValues(requestColumn.getValuesList());
        return document;
    }

    @Override
    protected Message.Builder createColumnBuilder() {
        return DoubleColumn.newBuilder();
    }

    @Override
    protected void addAllValuesToBuilder(Message.Builder builder) {
        ((DoubleColumn.Builder) builder).addAllValues(this.getValues());
    }

    @Override
    protected DataValue createDataValueFromScalar(Double value) {
        return DataValue.newBuilder().setDoubleValue(value).build();
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        DoubleColumn doubleColumn = (DoubleColumn) toProtobufColumn();
        bucketBuilder.setDoubleColumn(doubleColumn);
    }

}
