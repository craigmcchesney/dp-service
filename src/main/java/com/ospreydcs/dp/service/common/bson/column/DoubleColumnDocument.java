package com.ospreydcs.dp.service.common.bson.column;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DoubleColumn;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.util.List;

@BsonDiscriminator(key = "_t", value = "doubleColumn")
public class DoubleColumnDocument extends ColumnDocumentBase {

    // instance variables
    private List<Double> values;

    public List<Double> getValues() {
        return values;
    }

    public void setValues(List<Double> values) {
        this.values = values;
    }

    public static ColumnDocumentBase fromDoubleColumn(DoubleColumn requestColumn) {
        DoubleColumnDocument document = new DoubleColumnDocument();
        document.setName(requestColumn.getName());
        document.setValues(requestColumn.getValuesList());
        return document;
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        DoubleColumn doubleColumn = this.toDoubleColumn();
        bucketBuilder.setDoubleColumn(doubleColumn);
    }

    @Override
    public DataColumn toDataColumn() throws DpException {
        return null;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }

    private DoubleColumn toDoubleColumn() {
        final DoubleColumn.Builder columnBuilder = DoubleColumn.newBuilder();
        columnBuilder.setName(this.getName());
        columnBuilder.addAllValues(this.getValues());
        return columnBuilder.build();
    }

}
