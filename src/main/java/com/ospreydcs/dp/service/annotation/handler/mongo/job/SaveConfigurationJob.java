package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.SaveConfigurationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveConfigurationResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.SaveConfigurationDispatcher;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.MongoSaveResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

public class SaveConfigurationJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final SaveConfigurationRequest request;
    private final StreamObserver<SaveConfigurationResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final SaveConfigurationDispatcher dispatcher;

    public SaveConfigurationJob(
            SaveConfigurationRequest request,
            StreamObserver<SaveConfigurationResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new SaveConfigurationDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing SaveConfigurationJob id: {}", responseObserver.hashCode());

        // validate configurationName not blank
        if (request.getConfigurationName().isBlank()) {
            dispatcher.handleValidationError(
                    new ResultStatus(true, "SaveConfigurationRequest.configurationName must be specified"));
            return;
        }

        // validate category not blank
        if (request.getCategory().isBlank()) {
            dispatcher.handleValidationError(
                    new ResultStatus(true, "SaveConfigurationRequest.category must be specified"));
            return;
        }

        // check for duplicate attribute keys
        final Set<String> attributeKeys = new HashSet<>();
        for (var attr : request.getAttributesList()) {
            if (!attributeKeys.add(attr.getName())) {
                dispatcher.handleValidationError(new ResultStatus(
                        true, "SaveConfigurationRequest.attributes contains duplicate key: " + attr.getName()));
                return;
            }
        }

        // build document and upsert
        final ConfigurationDocument document = ConfigurationDocument.fromSaveConfigurationRequest(request);
        final MongoSaveResult result = mongoClient.saveConfiguration(document);
        dispatcher.handleResult(result);
    }
}
