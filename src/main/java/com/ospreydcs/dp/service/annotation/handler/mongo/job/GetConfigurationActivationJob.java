package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.GetConfigurationActivationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.GetConfigurationActivationResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.GetConfigurationActivationDispatcher;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;
import com.mongodb.MongoException;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;

public class GetConfigurationActivationJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final GetConfigurationActivationRequest request;
    private final StreamObserver<GetConfigurationActivationResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final GetConfigurationActivationDispatcher dispatcher;

    public GetConfigurationActivationJob(
            GetConfigurationActivationRequest request,
            StreamObserver<GetConfigurationActivationResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new GetConfigurationActivationDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing GetConfigurationActivationJob id: {}", responseObserver.hashCode());

        ConfigurationActivationDocument document = null;

        try {
            switch (request.getKeyCase()) {
                case CLIENTACTIVATIONID -> {
                    if (request.getClientActivationId().isBlank()) {
                        dispatcher.handleValidationError(new ResultStatus(
                                true, "GetConfigurationActivationRequest.clientActivationId must not be blank"));
                        return;
                    }
                    document = mongoClient.findConfigurationActivationById(request.getClientActivationId());
                }
                case COMPOSITEKEY -> {
                    final GetConfigurationActivationRequest.CompositeKey compositeKey = request.getCompositeKey();
                    if (compositeKey.getConfigurationName().isBlank()) {
                        dispatcher.handleValidationError(new ResultStatus(
                                true, "GetConfigurationActivationRequest.compositeKey.configurationName must not be blank"));
                        return;
                    }
                    if (!compositeKey.hasStartTime()
                            || (compositeKey.getStartTime().getEpochSeconds() == 0
                                && compositeKey.getStartTime().getNanoseconds() == 0)) {
                        dispatcher.handleValidationError(new ResultStatus(
                                true, "GetConfigurationActivationRequest.compositeKey.startTime must be specified"));
                        return;
                    }
                    final Instant startTime = TimestampUtility.instantFromTimestamp(compositeKey.getStartTime());
                    document = mongoClient.findConfigurationActivationByCompositeKey(
                            compositeKey.getConfigurationName(), startTime);
                }
                case KEY_NOT_SET -> {
                    dispatcher.handleValidationError(new ResultStatus(
                            true, "GetConfigurationActivationRequest must specify a key (clientActivationId or compositeKey)"));
                    return;
                }
            }
        } catch (MongoException ex) {
            dispatcher.handleError("error looking up ConfigurationActivation: " + ex.getMessage());
            return;
        }

        dispatcher.handleResult(document);
    }
}
