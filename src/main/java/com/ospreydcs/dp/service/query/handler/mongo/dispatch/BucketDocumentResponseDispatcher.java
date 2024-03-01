package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class BucketDocumentResponseDispatcher extends Dispatcher {

    private static final Logger logger = LogManager.getLogger();

    private StreamObserver<QueryDataResponse> responseObserver;

    public BucketDocumentResponseDispatcher() {
    }

    public BucketDocumentResponseDispatcher(StreamObserver<QueryDataResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    protected abstract void handleResult_(MongoCursor<BucketDocument> cursor);

    public StreamObserver<QueryDataResponse> getResponseObserver() {
        return this.responseObserver;
    }

    public void handleResult(MongoCursor<BucketDocument> cursor) {

        // send error response and close response stream if cursor is null
        if (cursor == null) {
            final String msg = "executeQuery returned null cursor";
            logger.error(msg);
            QueryServiceImpl.sendQueryResponseDataError(msg, getResponseObserver());
            return;
        }

        // send empty QueryStatus and close response stream if query matched no data
        if (!cursor.hasNext()) {
            logger.trace("processQueryRequest: query matched no data, cursor is empty");
            QueryServiceImpl.sendQueryResponseDataEmpty(getResponseObserver());
            return;
        }

        handleResult_(cursor);
    }

    protected QueryDataResponse nextQueryResponseFromCursor(MongoCursor<BucketDocument> cursor) {

        // build response from query result cursor
        QueryDataResponse.QueryResult.QueryData.Builder queryDataBuilder =
                QueryDataResponse.QueryResult.QueryData.newBuilder();

        int messageSize = 0;
        while (cursor.hasNext()){

            final BucketDocument document = cursor.next();
            final QueryDataResponse.QueryResult.QueryData.DataBucket bucket =
                    MongoQueryHandler.dataBucketFromDocument(document);

            // determine bucket size and check if too large
            int bucketSerializedSize = bucket.getSerializedSize();
            if (bucketSerializedSize > MongoQueryHandler.MAX_GRPC_MESSAGE_SIZE) {
                // single bucket is larger than maximum message size, so send error response
                return QueryServiceImpl.queryResponseDataError(
                        "bucket size: " + bucketSerializedSize
                                + " greater than maximum message size: " + MongoQueryHandler.MAX_GRPC_MESSAGE_SIZE);
            }

            // add bucket to result
            queryDataBuilder.addDataBuckets(bucket);
            messageSize = messageSize + bucketSerializedSize;

            // break out of cursor handling loop if next bucket might exceed maximum size
            if (messageSize + bucketSerializedSize > MongoQueryHandler.MAX_GRPC_MESSAGE_SIZE) {
                break;
            }
        }

        if (messageSize > 0) {
            // create response from buckets in result
            return QueryServiceImpl.queryResponseData(queryDataBuilder);
        }

        return null;
    }

}
