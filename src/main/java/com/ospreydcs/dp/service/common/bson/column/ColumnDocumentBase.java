package com.ospreydcs.dp.service.common.bson.column;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator
public abstract class ColumnDocumentBase {

    public abstract void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException;

    // TODO: this is for the tabular query result mechanism.  It works, but may be a pain for subclasses to generate from their native data.
    public abstract DataColumn toDataColumn() throws DpException;

    // TODO: this is for the export to hdf5 mechanism.  It works, but may be a pain for subclasses depending on the native format.
    public abstract byte[] getBytes();
}
