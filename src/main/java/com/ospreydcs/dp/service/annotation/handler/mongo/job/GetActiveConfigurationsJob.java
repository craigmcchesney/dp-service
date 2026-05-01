package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.GetActiveConfigurationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.GetActiveConfigurationsResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.GetActiveConfigurationsDispatcher;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.ConfigurationActivationQueryResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;

public class GetActiveConfigurationsJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final GetActiveConfigurationsRequest request;
    private final StreamObserver<GetActiveConfigurationsResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final GetActiveConfigurationsDispatcher dispatcher;

    public GetActiveConfigurationsJob(
            GetActiveConfigurationsRequest request,
            StreamObserver<GetActiveConfigurationsResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new GetActiveConfigurationsDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing GetActiveConfigurationsJob id: {}", responseObserver.hashCode());

        // validate timestamp is present and non-zero
        if (!request.hasTimestamp()
                || (request.getTimestamp().getEpochSeconds() == 0 && request.getTimestamp().getNanoseconds() == 0)) {
            dispatcher.handleValidationError(new ResultStatus(
                    true, "timestamp is required; supply the explicit point in time to query"));
            return;
        }

        final Instant timestamp = TimestampUtility.instantFromTimestamp(request.getTimestamp());
        final ConfigurationActivationQueryResult result = mongoClient.getActiveConfigurations(timestamp);
        if (result == null) {
            dispatcher.handleError("error executing getActiveConfigurations query");
            return;
        }
        dispatcher.handleResult(result);
    }
}
