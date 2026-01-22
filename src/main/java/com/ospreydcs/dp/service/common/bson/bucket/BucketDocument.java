package com.ospreydcs.dp.service.common.bson.bucket;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.service.common.bson.DataColumnDocument;
import com.ospreydcs.dp.service.common.bson.DataTimestampsDocument;
import com.ospreydcs.dp.service.common.bson.DpBsonDocumentBase;
import com.ospreydcs.dp.service.common.bson.EventMetadataDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import com.ospreydcs.dp.service.ingest.model.DpIngestionException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This POJO is for writing time series data to mongodb by customizing the code registry.
 *
 * NOTE: DATABASE CODE LIKE insertMany SILENTLY FAILS IF AN INSTANCE VARIABLE IS ADDED WITHOUT ACCESSOR METHODS!!!
 */
public class BucketDocument extends DpBsonDocumentBase {

    // instance variables
    private String id;
    private String pvName;
    private DataColumnDocument dataColumn;
    private DataTimestampsDocument dataTimestamps;
    private String providerId;
    private String providerName;
    private String clientRequestId;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPvName() {
        return pvName;
    }

    public void setPvName(String pvName) {
        this.pvName = pvName;
    }

    public DataColumnDocument getDataColumn() {
        return dataColumn;
    }

    public void setDataColumn(DataColumnDocument dataColumn) {
        this.dataColumn = dataColumn;
    }

    public DataTimestampsDocument getDataTimestamps() {
        return dataTimestamps;
    }

    public void setDataTimestamps(DataTimestampsDocument dataTimestamps) {
        this.dataTimestamps = dataTimestamps;
    }

    public String getProviderId() {
        return providerId;
    }

    public void setProviderId(String providerId) {
        this.providerId = providerId;
    }

    public String getProviderName() {
        return providerName;
    }

    public void setProviderName(String providerName) {
        this.providerName = providerName;
    }

    public String getClientRequestId() {
        return clientRequestId;
    }

    public void setClientRequestId(String clientRequestId) {
        this.clientRequestId = clientRequestId;
    }

    private static BucketDocument columnBucketDocument(
            String pvName,
            IngestDataRequest request,
            DataColumnDocument dataColumnDocument,
            String providerName
    ) {
        final BucketDocument bucket = new BucketDocument();

        // create DataTimestampsDocument for the request
        final DataTimestampsDocument requestDataTimestampsDocument =
                DataTimestampsDocument.fromDataTimestamps(request.getIngestionDataFrame().getDataTimestamps());

        // get PV name and generate id for BucketDocument
        final String documentId = pvName + "-"
                + requestDataTimestampsDocument.getFirstTime().getSeconds() + "-"
                + requestDataTimestampsDocument.getFirstTime().getNanos();
        bucket.setId(documentId);
        bucket.setPvName(pvName);
        bucket.setProviderId(request.getProviderId());
        bucket.setProviderName(providerName);
        bucket.setClientRequestId(request.getClientRequestId());

        bucket.setDataColumn(dataColumnDocument);

        // embed requestDataTimesetampsDocument within each BucketDocument
        bucket.setDataTimestamps(requestDataTimestampsDocument);

        // add tags
        if ( ! request.getTagsList().isEmpty()) {
            bucket.setTags(request.getTagsList());
        }

        // add attributes
        if ( ! request.getAttributesList().isEmpty()) {
            final Map<String, String> attributeMap =
                    AttributesUtility.attributeMapFromList(request.getAttributesList());
            bucket.setAttributes(attributeMap);
        }

        // create EventMetadataDocument for request EventMetadata
        if (request.hasEventMetadata()) {
            EventMetadataDocument eventMetadataDocument =
                    EventMetadataDocument.fromEventMetadata(request.getEventMetadata());
            bucket.setEvent(eventMetadataDocument);
        }

        return bucket;
    }

    private static BucketDocument dataColumnBucketDocument(
            IngestDataRequest request,
            DataColumn column,
            String providerName
    ) {
        // create DataColumnDocument for request DataColumn
        DataColumnDocument dataColumnDocument = DataColumnDocument.fromDataColumn(column);
        final String pvName = column.getName();
        return columnBucketDocument(pvName, request, dataColumnDocument, providerName);
    }

    private static BucketDocument serializedDataColumnBucketDocument(
            IngestDataRequest request,
            SerializedDataColumn column,
            String providerName
    ) {
        // create DataColumnDocument for request DataColumn
        DataColumnDocument dataColumnDocument = DataColumnDocument.fromSerializedDataColumn(column);
        final String pvName = column.getName();
        return columnBucketDocument(pvName, request, dataColumnDocument, providerName);
    }

    /**
     * Generates a list of POJO objects, which are written as a batch to mongodb by customizing the codec registry.
     * <p>
     * NOTE: DATABASE CODE LIKE insertMany SILENTLY FAILS IF AN INSTANCE VARIABLE IS ADDED TO TsDataBucket
     * WITHOUT ACCESSOR METHODS!!!  Very hard to troubleshoot.
     *
     * @param request
     * @param providerName
     * @return
     */
    public static List<BucketDocument> generateBucketsFromRequest(IngestDataRequest request, String providerName)
            throws DpIngestionException {

        final List<BucketDocument> bucketList = new ArrayList<>();

        // create BucketDocument for each DataColumn
        for (DataColumn column : request.getIngestionDataFrame().getDataColumnsList()) {
            bucketList.add(dataColumnBucketDocument(request, column, providerName));
        }

        // create BucketDocument for each SerializedDataColumn
        for (SerializedDataColumn column :
                request.getIngestionDataFrame().getSerializedDataColumnsList()) {
            bucketList.add(serializedDataColumnBucketDocument(request, column, providerName));
        }

        return bucketList;
    }

    public static DataBucket dataBucketFromDocument(
            BucketDocument document,
            QueryDataRequest.QuerySpec querySpec
    ) throws DpException {

        final DataBucket.Builder bucketBuilder = DataBucket.newBuilder();

        // add data timestamps
        DataTimestamps dataTimestamps = document.getDataTimestamps().toDataTimestamps();
        bucketBuilder.setDataTimestamps(dataTimestamps);

        // add data values
//        if (querySpec.getUseSerializedDataColumns()) {
//            SerializedDataColumn serializedDataColumn = document.getDataColumn().toSerializedDataColumn();
//            bucketBuilder.setSerializedDataColumn(serializedDataColumn);
//        } else {
//            DataColumn dataColumn = document.getDataColumn().toDataColumn();
//            bucketBuilder.setDataColumn(dataColumn);
//        }
        DataColumn dataColumn = document.getDataColumn().toDataColumn();
        bucketBuilder.setDataColumn(dataColumn);

        // add tags
        if (document.getTags() != null) {
            bucketBuilder.addAllTags(document.getTags());
        }

        // add attributes
        if (document.getAttributes() != null) {
            for (var documentAttributeMapEntry : document.getAttributes().entrySet()) {
                final String documentAttributeKey = documentAttributeMapEntry.getKey();
                final String documentAttributeValue = documentAttributeMapEntry.getValue();
                final Attribute responseAttribute = Attribute.newBuilder()
                        .setName(documentAttributeKey)
                        .setValue(documentAttributeValue)
                        .build();
                bucketBuilder.addAttributes(responseAttribute);
            }
        }

        // add event metadata
        if (document.getEvent() != null) {
            final EventMetadataDocument eventMetadataDocument = document.getEvent();
            bucketBuilder.setEventMetadata(eventMetadataDocument.toEventMetadata());
        }

        // add provider details
        if (document.getProviderId() != null) {
            bucketBuilder.setProviderId(document.getProviderId());
        }
        if (document.getProviderName() != null) {
            bucketBuilder.setProviderName(document.getProviderName());
        }

        return bucketBuilder.build();
    }

}
