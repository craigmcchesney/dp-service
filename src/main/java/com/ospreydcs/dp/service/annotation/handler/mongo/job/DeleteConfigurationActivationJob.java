package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.DeleteConfigurationActivationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.DeleteConfigurationActivationResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.DeleteConfigurationActivationDispatcher;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.MongoDeleteResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;

public class DeleteConfigurationActivationJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final DeleteConfigurationActivationRequest request;
    private final StreamObserver<DeleteConfigurationActivationResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final DeleteConfigurationActivationDispatcher dispatcher;

    public DeleteConfigurationActivationJob(
            DeleteConfigurationActivationRequest request,
            StreamObserver<DeleteConfigurationActivationResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new DeleteConfigurationActivationDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing DeleteConfigurationActivationJob id: {}", responseObserver.hashCode());

        MongoDeleteResult result = null;

        switch (request.getKeyCase()) {
            case CLIENTACTIVATIONID -> {
                if (request.getClientActivationId().isBlank()) {
                    dispatcher.handleValidationError(new ResultStatus(
                            true, "DeleteConfigurationActivationRequest.clientActivationId must not be blank"));
                    return;
                }
                result = mongoClient.deleteConfigurationActivation(request.getClientActivationId());
            }
            case COMPOSITEKEY -> {
                final DeleteConfigurationActivationRequest.CompositeKey compositeKey = request.getCompositeKey();
                if (compositeKey.getConfigurationName().isBlank()) {
                    dispatcher.handleValidationError(new ResultStatus(
                            true, "DeleteConfigurationActivationRequest.compositeKey.configurationName must not be blank"));
                    return;
                }
                if (!compositeKey.hasStartTime()
                        || (compositeKey.getStartTime().getEpochSeconds() == 0
                            && compositeKey.getStartTime().getNanoseconds() == 0)) {
                    dispatcher.handleValidationError(new ResultStatus(
                            true, "DeleteConfigurationActivationRequest.compositeKey.startTime must be specified"));
                    return;
                }
                final Instant startTime = TimestampUtility.instantFromTimestamp(compositeKey.getStartTime());
                result = mongoClient.deleteConfigurationActivationByCompositeKey(
                        compositeKey.getConfigurationName(), startTime);
            }
            case KEY_NOT_SET -> {
                dispatcher.handleValidationError(new ResultStatus(
                        true, "DeleteConfigurationActivationRequest must specify a key (clientActivationId or compositeKey)"));
                return;
            }
        }

        dispatcher.handleResult(result);
    }
}
