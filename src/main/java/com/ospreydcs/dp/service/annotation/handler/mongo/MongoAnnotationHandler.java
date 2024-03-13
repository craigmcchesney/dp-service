package com.ospreydcs.dp.service.annotation.handler.mongo;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationResponse;
import com.ospreydcs.dp.grpc.v1.annotation.DataBlock;
import com.ospreydcs.dp.grpc.v1.annotation.DataSet;
import com.ospreydcs.dp.service.annotation.handler.interfaces.AnnotationHandlerInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoSyncAnnotationClient;
import com.ospreydcs.dp.service.annotation.handler.mongo.job.CreateCommentAnnotationJob;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.handler.QueueHandlerBase;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MongoAnnotationHandler extends QueueHandlerBase implements AnnotationHandlerInterface {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // configuration
    public static final String CFG_KEY_NUM_WORKERS = "AnnotationHandler.numWorkers";
    public static final int DEFAULT_NUM_WORKERS = 7;

    // instance variables
    private final MongoAnnotationClientInterface mongoAnnotationClient;
    private final MongoQueryClientInterface mongoQueryClient;

    public MongoAnnotationHandler(
            MongoAnnotationClientInterface mongoAnnotationClient,
            MongoQueryClientInterface mongoQueryClient
    ) {
        this.mongoAnnotationClient = mongoAnnotationClient;
        this.mongoQueryClient = mongoQueryClient;
    }

    public static MongoAnnotationHandler newMongoSyncAnnotationHandler() {
        return new MongoAnnotationHandler(
                new MongoSyncAnnotationClient(), new MongoSyncQueryClient());
    }

    protected int getNumWorkers_() {
        return configMgr().getConfigInteger(CFG_KEY_NUM_WORKERS, DEFAULT_NUM_WORKERS);
    }

    @Override
    protected boolean init_() {
        logger.trace("init_");
        if (!mongoAnnotationClient.init()) {
            logger.error("error in mongoAnnotationClient.init");
            return false;
        }
        if (!mongoQueryClient.init()) {
            logger.error("error in mongoQueryClient.init");
        }
        return true;
    }

    @Override
    protected boolean fini_() {
        if (!mongoQueryClient.fini()) {
            logger.error("error in mongoQueryClient.fini");
        }
        if (!mongoAnnotationClient.fini()) {
            logger.error("error in mongoAnnotationClient.fini");
        }
        return true;
    }

    @Override
    public void handleCreateCommentAnnotationRequest(
            CreateAnnotationRequest request,
            StreamObserver<CreateAnnotationResponse> responseObserver
    ) {
        final CreateCommentAnnotationJob job = new CreateCommentAnnotationJob(
                request,
                responseObserver,
                mongoAnnotationClient,
                this);

        logger.debug("adding CreateCommentAnnotationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    public ValidationResult validateAnnotationRequest(CreateAnnotationRequest request) {

        // create list of unique pv names in DataSet's DataBlocks using a set, convert set to list
        final Set<String> uniquePvNames = new HashSet<>();
        if (request.getDataSet() == null) {
            return new ValidationResult(true, "CreateAnnotationRequest must contain a DataSet");
        }
        final DataSet dataSet = request.getDataSet();
        final List<DataBlock> dataBlocks = dataSet.getDataBlocksList();
        if (dataBlocks == null || dataBlocks.isEmpty()) {
            return new ValidationResult(true, "CreateAnnotationRequest DataSet must contain DataBlocks");
        }
        for (DataBlock dataBlock : dataBlocks) {
            List<String> blockPvNames = dataBlock.getPvNamesList();
            if (blockPvNames == null || blockPvNames.isEmpty()) {
                return new ValidationResult(true, "DataBlock must contain pvNames");
            }
            uniquePvNames.addAll(blockPvNames);
        }

        // execute metadata query for list of pv names
        final MongoCursor<Document> pvMetadata = mongoQueryClient.executeQueryMetadata(uniquePvNames);
        if (pvMetadata == null) {
            return new ValidationResult(true, "error executing pv metadata query to validate request");
        }

        // check that metadata is returned for each pv (try to remove each metadata from the set,
        // and make sure set end up empty)
        while (pvMetadata.hasNext()) {
            final Document pvMetadataDocument = pvMetadata.next();
            final String pvName = (String) pvMetadataDocument.get(BsonConstants.BSON_KEY_BUCKET_NAME);
            if (pvName != null) {
                uniquePvNames.remove(pvName);
            }
        }

        // we should have removed all the pv names from the set of unique names, e.g., we received metadata for each
        if (uniquePvNames.isEmpty()) {
            return new ValidationResult(false, "");
        } else {
            return new ValidationResult(true, "no PV metadata found for names: " + uniquePvNames.toString());
        }
    }

}
