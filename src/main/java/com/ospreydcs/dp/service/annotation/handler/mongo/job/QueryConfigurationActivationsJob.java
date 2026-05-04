package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationActivationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationActivationsResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.QueryConfigurationActivationsDispatcher;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.ConfigurationActivationQueryResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryConfigurationActivationsJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final QueryConfigurationActivationsRequest request;
    private final StreamObserver<QueryConfigurationActivationsResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final QueryConfigurationActivationsDispatcher dispatcher;

    public QueryConfigurationActivationsJob(
            QueryConfigurationActivationsRequest request,
            StreamObserver<QueryConfigurationActivationsResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new QueryConfigurationActivationsDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing QueryConfigurationActivationsJob id: {}", responseObserver.hashCode());

        // validate criteria list not empty
        if (request.getCriteriaList().isEmpty()) {
            dispatcher.handleValidationError(new ResultStatus(
                    true, "QueryConfigurationActivationsRequest.criteria must not be empty"));
            return;
        }

        // validate each criterion
        for (var criterion : request.getCriteriaList()) {
            switch (criterion.getCriterionCase()) {
                case TIMESTAMPCRITERION -> {
                    final var tc = criterion.getTimestampCriterion();
                    if (!tc.hasTimestamp()
                            || (tc.getTimestamp().getEpochSeconds() == 0 && tc.getTimestamp().getNanoseconds() == 0)) {
                        dispatcher.handleValidationError(new ResultStatus(
                                true, "TimestampCriterion.timestamp must be specified"));
                        return;
                    }
                }
                case TIMERANGECRITERION -> {
                    final var trc = criterion.getTimeRangeCriterion();
                    if (!trc.hasStartTime()
                            || (trc.getStartTime().getEpochSeconds() == 0 && trc.getStartTime().getNanoseconds() == 0)) {
                        dispatcher.handleValidationError(new ResultStatus(
                                true, "TimeRangeCriterion.startTime must be specified"));
                        return;
                    }
                    if (!trc.hasEndTime()
                            || (trc.getEndTime().getEpochSeconds() == 0 && trc.getEndTime().getNanoseconds() == 0)) {
                        dispatcher.handleValidationError(new ResultStatus(
                                true, "TimeRangeCriterion.endTime must be specified"));
                        return;
                    }
                }
                case CONFIGURATIONNAMECRITERION -> {
                    if (criterion.getConfigurationNameCriterion().getValuesList().isEmpty()) {
                        dispatcher.handleValidationError(new ResultStatus(
                                true, "ConfigurationNameCriterion.values must not be empty"));
                        return;
                    }
                }
                case CLIENTACTIVATIONIDCRITERION -> {
                    if (criterion.getClientActivationIdCriterion().getValuesList().isEmpty()) {
                        dispatcher.handleValidationError(new ResultStatus(
                                true, "ClientActivationIdCriterion.values must not be empty"));
                        return;
                    }
                }
                case CATEGORYCRITERION -> {
                    if (criterion.getCategoryCriterion().getValuesList().isEmpty()) {
                        dispatcher.handleValidationError(new ResultStatus(
                                true, "CategoryCriterion.values must not be empty"));
                        return;
                    }
                }
                case TAGSCRITERION -> {
                    if (criterion.getTagsCriterion().getValuesList().isEmpty()) {
                        dispatcher.handleValidationError(new ResultStatus(
                                true, "TagsCriterion.values must not be empty"));
                        return;
                    }
                }
                case ATTRIBUTESCRITERION -> {
                    if (criterion.getAttributesCriterion().getKey().isBlank()) {
                        dispatcher.handleValidationError(new ResultStatus(
                                true, "AttributesCriterion.key must not be blank"));
                        return;
                    }
                }
                case CRITERION_NOT_SET -> {
                    dispatcher.handleValidationError(new ResultStatus(
                            true, "QueryConfigurationActivationsRequest.criteria contains an unset criterion"));
                    return;
                }
            }
        }

        final ConfigurationActivationQueryResult result = mongoClient.executeQueryConfigurationActivations(request);
        if (result == null) {
            dispatcher.handleError("error executing query for configuration activations");
            return;
        }
        dispatcher.handleResult(result);
    }
}
