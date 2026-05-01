package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.GetConfigurationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.GetConfigurationResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.GetConfigurationDispatcher;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GetConfigurationJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final GetConfigurationRequest request;
    private final StreamObserver<GetConfigurationResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final GetConfigurationDispatcher dispatcher;

    public GetConfigurationJob(
            GetConfigurationRequest request,
            StreamObserver<GetConfigurationResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new GetConfigurationDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing GetConfigurationJob id: {}", responseObserver.hashCode());

        if (request.getConfigurationName().isBlank()) {
            dispatcher.handleValidationError(
                    new ResultStatus(true, "GetConfigurationRequest.configurationName must be specified"));
            return;
        }

        final ConfigurationDocument document;
        try {
            document = mongoClient.findConfigurationByName(request.getConfigurationName());
        } catch (Exception ex) {
            dispatcher.handleError("error looking up Configuration: " + ex.getMessage());
            return;
        }
        dispatcher.handleResult(document);
    }
}
