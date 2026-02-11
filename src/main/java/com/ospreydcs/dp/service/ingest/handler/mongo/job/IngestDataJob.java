package com.ospreydcs.dp.service.ingest.handler.mongo.job;

import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionResult;
import com.ospreydcs.dp.service.ingest.handler.mongo.client.MongoIngestionClientInterface;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionHandler;
import com.ospreydcs.dp.service.ingest.model.DpIngestionException;
import com.ospreydcs.dp.service.ingest.model.IngestionRequestStatus;
import com.ospreydcs.dp.service.ingest.model.IngestionTaskResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class IngestDataJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final HandlerIngestionRequest request;
    private final MongoIngestionClientInterface mongoClient;
    private final MongoIngestionHandler handler;

    public IngestDataJob(
            HandlerIngestionRequest request,
            MongoIngestionClientInterface mongoClient,
            MongoIngestionHandler handler
    ) {
        this.request = request;
        this.mongoClient = mongoClient;
        this.handler = handler;
    }

    @Override
    public void execute() {
        this.handleIngestionRequest(request);
    }

    public HandlerIngestionResult handleIngestionRequest(HandlerIngestionRequest handlerIngestionRequest) {

        final IngestDataRequest request = handlerIngestionRequest.request;
        logger.debug("id: {} handling ingestion request providerId: {} requestId: {}",
                this.hashCode(), request.getProviderId(), request.getClientRequestId());

        IngestionRequestStatus status = IngestionRequestStatus.SUCCESS;
        boolean isError = false;
        String errorMsg = "";
        List<String> idsCreated = new ArrayList<>();

        // validate providerId by getting providerName
        String providerName = mongoClient.providerNameForId(request.getProviderId());

        if (handlerIngestionRequest.rejected) {
            // request already rejected, but we want to add details in request status
            isError = true;
            errorMsg = handlerIngestionRequest.rejectMsg;
            status = IngestionRequestStatus.REJECTED;

        } else {

            // flag error for invalid providerId
            if (providerName == null) {
                isError = true;
                errorMsg = "invalid providerId: " + request.getProviderId();
                logger.error(errorMsg);

            } else {

                // generate batch of bucket documents for request
                List<BucketDocument> dataDocumentBatch = null;
                try {
                    dataDocumentBatch = BucketDocument.generateBucketsFromRequest(request, providerName);
                } catch (DpIngestionException e) {
                    isError = true;
                    errorMsg = e.getMessage();
                    status = IngestionRequestStatus.ERROR;
                }

                if (dataDocumentBatch != null) {
                    // add the batch to mongo and handle result
                    IngestionTaskResult ingestionTaskResult =
                            mongoClient.insertBatch(request, dataDocumentBatch);

                    if (ingestionTaskResult.isError) {
                        isError = true;
                        errorMsg = ingestionTaskResult.msg;
                        logger.error(errorMsg);

                    } else {

                        InsertManyResult insertManyResult = ingestionTaskResult.insertManyResult;

                        if (!insertManyResult.wasAcknowledged()) {
                            // check mongo insertMany result was acknowledged
                            isError = true;
                            errorMsg = "insertMany result not acknowledged";
                            logger.error(errorMsg);

                        } else {

                            long recordsInsertedCount = insertManyResult.getInsertedIds().size();
                            long recordsExpected = dataDocumentBatch.size();

                            if (recordsInsertedCount != recordsExpected) {
                                // check records inserted matches expected
                                isError = true;
                                errorMsg = "insertMany actual records inserted: "
                                        + recordsInsertedCount + " mismatch expected: " + recordsExpected;
                                logger.error(errorMsg);

                            } else {
                                // get list of ids created
                                for (var entry : insertManyResult.getInsertedIds().entrySet()) {
                                    idsCreated.add(entry.getValue().asString().getValue());
                                }
                            }
                        }
                    }
                }
            }

            if (isError) {
                status = IngestionRequestStatus.ERROR;

            }
        }
        
        // save request status and check result of insert operation
        if (providerName == null) {
            providerName = "";
        }
        RequestStatusDocument statusDocument = new RequestStatusDocument(
                request.getProviderId(),
                providerName,
                request.getClientRequestId(),
                status,
                errorMsg,
                idsCreated);
        InsertOneResult insertRequestStatusResult = mongoClient.insertRequestStatus(statusDocument);
        if (insertRequestStatusResult == null) {
            logger.error("error inserting request status");
        } else {
            if (!insertRequestStatusResult.wasAcknowledged()) {
                logger.error("insertOne not acknowledged inserting request status");
            } else {
                logger.debug("inserted request status id:" + insertRequestStatusResult.getInsertedId());
            }
        }

        // publish request PV data to subscribeData() subscribers
        if (! isError) {
            handler.getSourceMonitorPublisher().publishDataSubscriptions(request);
        }

        return new HandlerIngestionResult(isError, errorMsg);
    }

}
