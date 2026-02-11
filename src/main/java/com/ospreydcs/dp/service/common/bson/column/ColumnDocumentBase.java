package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator
public abstract class ColumnDocumentBase {

    // instance variables
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    protected abstract Message.Builder createColumnBuilder();

    protected abstract void addAllValuesToBuilder(Message.Builder builder);

    private void setBuilderName(Message.Builder builder, String name) {
        try {
            // Handle null names by using empty string
            String safeName = (name != null) ? name : "";
            builder.getClass().getMethod("setName", String.class).invoke(builder, safeName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set name on column builder", e);
        }
    }

    public Message toProtobufColumn() {
        Message.Builder builder = createColumnBuilder();
        setBuilderName(builder, this.getName());
        addAllValuesToBuilder(builder);
        return builder.build();
    }

    public byte[] toByteArray() {
        return toProtobufColumn().toByteArray();
    }

    /**
     * Adds the column to the supplied DataBucket.Builder for use in query result.
     *
     * @param bucketBuilder
     * @throws DpException
     */
    public abstract void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException;

    // TODO: this is for the tabular query result mechanism.  It works, but may be a pain for subclasses to generate from their native data.
    public abstract DataColumn toDataColumn() throws DpException;
}
