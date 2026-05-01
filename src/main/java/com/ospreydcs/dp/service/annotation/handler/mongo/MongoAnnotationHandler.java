package com.ospreydcs.dp.service.annotation.handler.mongo;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.service.annotation.handler.interfaces.AnnotationHandlerInterface;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoSyncAnnotationClient;
import com.ospreydcs.dp.service.annotation.handler.mongo.job.*;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.PvMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.handler.QueueHandlerBase;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

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
    public void handleSaveDataSet(
            SaveDataSetRequest request, 
            StreamObserver<SaveDataSetResponse> responseObserver
    ) {
        final SaveDataSetJob job = new SaveDataSetJob(
                request,
                responseObserver,
                mongoAnnotationClient,
                this);

        logger.debug("adding SaveDataSetJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryDataSets(
            QueryDataSetsRequest request,
            StreamObserver<QueryDataSetsResponse> responseObserver
    ) {
        final QueryDataSetsJob job =
                new QueryDataSetsJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding queryDataSets job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    public ResultStatus validateSaveDataSetRequest(SaveDataSetRequest request) {

        // create list of unique pv names in DataSet's DataBlocks using a set, convert set to list
        final Set<String> uniquePvNames = new TreeSet<>();
        if (request.getDataSet() == null) {
            return new ResultStatus(true, "SaveDataSetRequest must contain a DataSet");
        }
        final DataSet dataSet = request.getDataSet();
        final List<DataBlock> dataBlocks = dataSet.getDataBlocksList();
        if (dataBlocks == null || dataBlocks.isEmpty()) {
            return new ResultStatus(true, "SaveDataSetRequest.DataSet must contain DataBlocks");
        }
        for (DataBlock dataBlock : dataBlocks) {
            List<String> blockPvNames = dataBlock.getPvNamesList();
            if (blockPvNames == null || blockPvNames.isEmpty()) {
                return new ResultStatus(
                        true, "SaveDataSetRequest.DataSet.DataBlock must contain pvNames");
            }
            uniquePvNames.addAll(blockPvNames);
        }

        // execute metadata query for list of pv names
        final MongoCursor<PvMetadataQueryResultDocument> pvMetadata = mongoQueryClient.executeQueryPvStats(uniquePvNames);
        if (pvMetadata == null) {
            return new ResultStatus(true, "error executing pv metadata query to validate request");
        }

        // check that metadata is returned for each pv (try to remove each metadata from the set,
        // and make sure set end up empty)
        while (pvMetadata.hasNext()) {
            final PvMetadataQueryResultDocument pvMetadataDocument = pvMetadata.next();
            final String pvName = pvMetadataDocument.getPvName();
            if (pvName != null) {
                uniquePvNames.remove(pvName);
            }
        }

        // we should have removed all the pv names from the set of unique names, e.g., we received metadata for each
        if (uniquePvNames.isEmpty()) {
            return new ResultStatus(false, "");
        } else {
            return new ResultStatus(true, "no PV metadata found for names: " + uniquePvNames.toString());
        }
    }

    @Override
    public void handleSaveAnnotation(
            SaveAnnotationRequest request,
            StreamObserver<SaveAnnotationResponse> responseObserver
    ) {
        final SaveAnnotationJob job = new SaveAnnotationJob(
                request,
                responseObserver,
                mongoAnnotationClient,
                this);

        logger.debug("adding SaveAnnotationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    public ResultStatus validateSaveAnnotationRequest(SaveAnnotationRequest request) {

        // check that each id in dataSetIds exists in database
        for (String dataSetId : request.getDataSetIdsList()) {

            if (dataSetId.isBlank()) {
                final String errorMsg = "CreateAnnotationRequest.dataSetIds contains blank id string";
                return new ResultStatus(true, errorMsg);
            }

            // execute query to retrieve DataSetDocument with specified id
            final DataSetDocument dataSetDocument = mongoAnnotationClient.findDataSet(dataSetId);
            if (dataSetDocument == null) {
                return new ResultStatus(
                        true,
                        "no DataSetDocument found with id: " + dataSetId);
            }
        }

        // check that each id in annotationIds exists in database
        for (String annotationId : request.getAnnotationIdsList()) {

            if (annotationId.isBlank()) {
                final String errorMsg = "CreateAnnotationRequest.annotationIds contains blank id string";
                return new ResultStatus(true, errorMsg);
            }

            final AnnotationDocument annotationDocument = mongoAnnotationClient.findAnnotation(annotationId);
            if (annotationDocument == null) {
                return new ResultStatus(
                        true,
                        "no AnnotationDocument found with id: " + annotationId);

            }
        }


        return new ResultStatus(false, "");
    }

    @Override
    public void handleQueryAnnotations(
            QueryAnnotationsRequest request, StreamObserver<QueryAnnotationsResponse> responseObserver
    ) {
        final QueryAnnotationsJob job =
                new QueryAnnotationsJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding queryAnnotations job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleExportData(HandlerExportDataRequest handlerRequest) {

        ExportDataJobBase job = null;
        switch (handlerRequest.exportDataRequest.getOutputFormat()) {
            case EXPORT_FORMAT_UNSPECIFIED -> {
                // this should be caught in validation, but just in case...
                final String errorMsg = "ExportDataRequest.outputFormat must be specified";
                AnnotationServiceImpl.sendExportDataResponseError(errorMsg, handlerRequest.responseObserver);
            }
            case EXPORT_FORMAT_HDF5 -> {
                job = new ExportDataJobHdf5(handlerRequest, mongoAnnotationClient, mongoQueryClient);
            }
            case EXPORT_FORMAT_CSV -> {
                job = new ExportDataJobCsv(handlerRequest, mongoAnnotationClient, mongoQueryClient);
            }
            case EXPORT_FORMAT_XLSX -> {
                job = new ExportDataJobExcel(handlerRequest, mongoAnnotationClient, mongoQueryClient);
            }
            case UNRECOGNIZED -> {
                // this should be caught in validation, but just in case...
                final String errorMsg = "ExportDataRequest.outputFormat unrecognized value";
                AnnotationServiceImpl.sendExportDataResponseError(errorMsg, handlerRequest.responseObserver);
            }
        }
        Objects.requireNonNull(job);

        logger.debug("adding ExportDataJobBase id: {} to queue", handlerRequest.responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleSavePvMetadata(
            SavePvMetadataRequest request,
            StreamObserver<SavePvMetadataResponse> responseObserver
    ) {
        final SavePvMetadataJob job = new SavePvMetadataJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding SavePvMetadataJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryPvMetadata(
            QueryPvMetadataRequest request,
            StreamObserver<QueryPvMetadataResponse> responseObserver
    ) {
        final QueryPvMetadataJob job = new QueryPvMetadataJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding QueryPvMetadataJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleGetPvMetadata(
            GetPvMetadataRequest request,
            StreamObserver<GetPvMetadataResponse> responseObserver
    ) {
        final GetPvMetadataJob job = new GetPvMetadataJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding GetPvMetadataJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleDeletePvMetadata(
            DeletePvMetadataRequest request,
            StreamObserver<DeletePvMetadataResponse> responseObserver
    ) {
        final DeletePvMetadataJob job = new DeletePvMetadataJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding DeletePvMetadataJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleSaveConfiguration(
            SaveConfigurationRequest request,
            StreamObserver<SaveConfigurationResponse> responseObserver
    ) {
        final SaveConfigurationJob job = new SaveConfigurationJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding SaveConfigurationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleGetConfiguration(
            GetConfigurationRequest request,
            StreamObserver<GetConfigurationResponse> responseObserver
    ) {
        final GetConfigurationJob job = new GetConfigurationJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding GetConfigurationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryConfigurations(
            QueryConfigurationsRequest request,
            StreamObserver<QueryConfigurationsResponse> responseObserver
    ) {
        final QueryConfigurationsJob job = new QueryConfigurationsJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding QueryConfigurationsJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleDeleteConfiguration(
            DeleteConfigurationRequest request,
            StreamObserver<DeleteConfigurationResponse> responseObserver
    ) {
        final DeleteConfigurationJob job = new DeleteConfigurationJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding DeleteConfigurationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleSaveConfigurationActivation(
            SaveConfigurationActivationRequest request,
            StreamObserver<SaveConfigurationActivationResponse> responseObserver
    ) {
        final SaveConfigurationActivationJob job =
                new SaveConfigurationActivationJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding SaveConfigurationActivationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleGetConfigurationActivation(
            GetConfigurationActivationRequest request,
            StreamObserver<GetConfigurationActivationResponse> responseObserver
    ) {
        final GetConfigurationActivationJob job =
                new GetConfigurationActivationJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding GetConfigurationActivationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryConfigurationActivations(
            QueryConfigurationActivationsRequest request,
            StreamObserver<QueryConfigurationActivationsResponse> responseObserver
    ) {
        final QueryConfigurationActivationsJob job =
                new QueryConfigurationActivationsJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding QueryConfigurationActivationsJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleDeleteConfigurationActivation(
            DeleteConfigurationActivationRequest request,
            StreamObserver<DeleteConfigurationActivationResponse> responseObserver
    ) {
        final DeleteConfigurationActivationJob job =
                new DeleteConfigurationActivationJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding DeleteConfigurationActivationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleGetActiveConfigurations(
            GetActiveConfigurationsRequest request,
            StreamObserver<GetActiveConfigurationsResponse> responseObserver
    ) {
        final GetActiveConfigurationsJob job =
                new GetActiveConfigurationsJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding GetActiveConfigurationsJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

}
