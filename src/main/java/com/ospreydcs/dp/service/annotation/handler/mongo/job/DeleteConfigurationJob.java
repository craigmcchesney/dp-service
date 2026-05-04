package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.DeleteConfigurationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.DeleteConfigurationResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.DeleteConfigurationDispatcher;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.MongoDeleteResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DeleteConfigurationJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final DeleteConfigurationRequest request;
    private final StreamObserver<DeleteConfigurationResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final DeleteConfigurationDispatcher dispatcher;

    public DeleteConfigurationJob(
            DeleteConfigurationRequest request,
            StreamObserver<DeleteConfigurationResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new DeleteConfigurationDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing DeleteConfigurationJob id: {}", responseObserver.hashCode());

        if (request.getConfigurationName().isBlank()) {
            dispatcher.handleValidationError(
                    new ResultStatus(true, "DeleteConfigurationRequest.configurationName must be specified"));
            return;
        }

        final MongoDeleteResult result = mongoClient.deleteConfiguration(request.getConfigurationName());
        dispatcher.handleResult(result);
    }
}
