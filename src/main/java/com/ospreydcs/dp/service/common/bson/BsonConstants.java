package com.ospreydcs.dp.service.common.bson;

public class BsonConstants {
    
    // BSON field name constants
    public static final String BSON_KEY_PV_NAME = "pvName";
    public static final String BSON_KEY_TAGS = "tags";
    public static final String BSON_KEY_ATTRIBUTES = "attributes";
    public static final String BSON_KEY_EVENT = "event";
    public static final String BSON_KEY_EVENT_DESCRIPTION = "event.description";
    public static final String BSON_KEY_CREATED_AT = "createdAt";
    public static final String BSON_KEY_UPDATED_AT = "updatedAt";

    public static final String BSON_KEY_PROVIDER_ID = "_id";
    public static final String BSON_KEY_PROVIDER_NAME = "name";
    public static final String BSON_KEY_PROVIDER_DESCRIPTION = "description";

    public static final String BSON_KEY_BUCKET_ID = "_id";
    public static final String BSON_KEY_BUCKET_FIRST_TIME = "dataTimestamps.firstTime.dateTime";
    public static final String BSON_KEY_BUCKET_FIRST_TIME_SECS = "dataTimestamps.firstTime.seconds";
    public static final String BSON_KEY_BUCKET_FIRST_TIME_NANOS = "dataTimestamps.firstTime.nanos";
    public static final String BSON_KEY_BUCKET_LAST_TIME = "dataTimestamps.lastTime.dateTime";
    public static final String BSON_KEY_BUCKET_LAST_TIME_SECS = "dataTimestamps.lastTime.seconds";
    public static final String BSON_KEY_BUCKET_LAST_TIME_NANOS = "dataTimestamps.lastTime.nanos";
    public static final String BSON_KEY_BUCKET_DATA_TIMESTAMPS_CASE = "dataTimestamps.valueCase";
    public static final String BSON_KEY_BUCKET_DATA_TIMESTAMPS_TYPE = "dataTimestamps.valueType";
    public static final String BSON_KEY_BUCKET_SAMPLE_COUNT = "dataTimestamps.sampleCount";
    public static final String BSON_KEY_BUCKET_SAMPLE_PERIOD = "dataTimestamps.samplePeriod";
    public static final String BSON_KEY_BUCKET_DATA_TYPE = "dataColumn._t";
    public static final String BSON_KEY_BUCKET_PROVIDER_ID = "providerId";
    public static final String BSON_KEY_BUCKET_PROVIDER_NAME = "providerName";

    public static final String BSON_KEY_REQ_STATUS_ID = "_id";
    public static final String BSON_KEY_REQ_STATUS_PROVIDER_ID = "providerId";
    public static final String BSON_KEY_REQ_STATUS_PROVIDER_NAME = "providerName";
    public static final String BSON_KEY_REQ_STATUS_REQUEST_ID = "requestId";
    public static final String BSON_KEY_REQ_STATUS_STATUS = "requestStatusCase";

    public static final String BSON_KEY_DATA_SET_ID = "_id";
    public static final String BSON_KEY_DATA_SET_NAME = "name";
    public static final String BSON_KEY_DATA_SET_OWNER_ID = "ownerId";
    public static final String BSON_KEY_DATA_SET_DESCRIPTION = "description";
    public static final String BSON_KEY_DATA_SET_BLOCK_PV_NAMES = "dataBlocks.pvNames";

    public static final String BSON_KEY_ANNOTATION_ID = "_id";
    public static final String BSON_KEY_ANNOTATION_OWNER_ID = "ownerId";
    public static final String BSON_KEY_ANNOTATION_DATASET_IDS = "dataSetIds";
    public static final String BSON_KEY_ANNOTATION_NAME = "name";
    public static final String BSON_KEY_ANNOTATION_ANNOTATION_IDS = "annotationIds";
    public static final String BSON_KEY_ANNOTATION_COMMENT = "comment";

    public static final String BSON_KEY_CALCULATIONS_ID = "_id";

    public static final String BSON_KEY_PV_METADATA_PV_NAME = "pvName";
    public static final String BSON_KEY_PV_METADATA_LAST_BUCKET_ID = "lastBucketId";
    public static final String BSON_KEY_PV_METADATA_LAST_BUCKET_DATA_TYPE_CASE = "lastBucketDataTypeCase";
    public static final String BSON_KEY_PV_METADATA_LAST_BUCKET_DATA_TYPE = "lastBucketDataType";
    public static final String BSON_KEY_PV_METADATA_LAST_BUCKET_DATA_TIMESTAMPS_CASE = "lastBucketDataTimestampsCase";
    public static final String BSON_KEY_PV_METADATA_LAST_BUCKET_DATA_TIMESTAMPS_TYPE = "lastBucketDataTimestampsType";
    public static final String BSON_KEY_PV_METADATA_LAST_BUCKET_SAMPLE_COUNT = "lastBucketSampleCount";
    public static final String BSON_KEY_PV_METADATA_LAST_BUCKET_SAMPLE_PERIOD = "lastBucketSamplePeriod";
    public static final String BSON_KEY_PV_METADATA_FIRST_DATA_TIMESTAMP = "firstDataTimestamp";
    public static final String BSON_KEY_PV_METADATA_LAST_DATA_TIMESTAMP = "lastDataTimestamp";
    public static final String BSON_KEY_PV_METADATA_NUM_BUCKETS = "numBuckets";
    public static final String BSON_KEY_PV_METADATA_LAST_PROVIDER_ID = "lastProviderId";
    public static final String BSON_KEY_PV_METADATA_LAST_PROVIDER_NAME = "lastProviderName";

    public static final String BSON_KEY_PROVIDER_METADATA_PV_NAMES = "pvNames";
    public static final String BSON_KEY_PROVIDER_METADATA_FIRST_BUCKET_TIMESTAMP = "firstBucketTimestamp";
    public static final String BSON_KEY_PROVIDER_METADATA_LAST_BUCKET_TIMESTAMP = "lastBucketTimestamp";
    public static final String BSON_KEY_PROVIDER_METADATA_NUM_BUCKETS = "numBuckets";
}
