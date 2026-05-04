package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.SaveConfigurationActivationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveConfigurationActivationResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.SaveConfigurationActivationDispatcher;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.MongoSaveResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

public class SaveConfigurationActivationJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final SaveConfigurationActivationRequest request;
    private final StreamObserver<SaveConfigurationActivationResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final SaveConfigurationActivationDispatcher dispatcher;

    public SaveConfigurationActivationJob(
            SaveConfigurationActivationRequest request,
            StreamObserver<SaveConfigurationActivationResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new SaveConfigurationActivationDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing SaveConfigurationActivationJob id: {}", responseObserver.hashCode());

        // validate configurationName not blank
        if (request.getConfigurationName().isBlank()) {
            dispatcher.handleValidationError(new ResultStatus(
                    true, "SaveConfigurationActivationRequest.configurationName must be specified"));
            return;
        }

        // validate startTime is present and non-zero
        if (!request.hasStartTime()
                || (request.getStartTime().getEpochSeconds() == 0 && request.getStartTime().getNanoseconds() == 0)) {
            dispatcher.handleValidationError(new ResultStatus(
                    true, "SaveConfigurationActivationRequest.startTime must be specified"));
            return;
        }

        // validate endTime is after startTime if present
        if (request.hasEndTime()) {
            if (TimestampUtility.compare(request.getEndTime(), request.getStartTime()) <= 0) {
                dispatcher.handleValidationError(new ResultStatus(
                        true, "SaveConfigurationActivationRequest.endTime must be after startTime"));
                return;
            }
        }

        // check for duplicate attribute keys
        final Set<String> attributeKeys = new HashSet<>();
        for (var attr : request.getAttributesList()) {
            if (!attributeKeys.add(attr.getName())) {
                dispatcher.handleValidationError(new ResultStatus(
                        true, "SaveConfigurationActivationRequest.attributes contains duplicate key: "
                        + attr.getName()));
                return;
            }
        }

        // build document and upsert
        final ConfigurationActivationDocument document =
                ConfigurationActivationDocument.fromSaveConfigurationActivationRequest(request);
        final MongoSaveResult result = mongoClient.saveConfigurationActivation(document);
        dispatcher.handleResult(result);
    }
}
