package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationsResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.QueryConfigurationsDispatcher;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.ConfigurationQueryResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryConfigurationsJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final QueryConfigurationsRequest request;
    private final StreamObserver<QueryConfigurationsResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final QueryConfigurationsDispatcher dispatcher;

    public QueryConfigurationsJob(
            QueryConfigurationsRequest request,
            StreamObserver<QueryConfigurationsResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new QueryConfigurationsDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing QueryConfigurationsJob id: {}", responseObserver.hashCode());

        // validate: criteria list must not be empty
        if (request.getCriteriaList().isEmpty()) {
            dispatcher.handleValidationError(
                    new ResultStatus(true, "QueryConfigurationsRequest.criteria list must not be empty"));
            return;
        }

        // validate each criterion
        for (QueryConfigurationsRequest.QueryConfigurationsCriterion criterion : request.getCriteriaList()) {
            switch (criterion.getCriterionCase()) {
                case NAMECRITERION -> {
                    final var c = criterion.getNameCriterion();
                    if (c.getExactList().isEmpty() && c.getPrefixList().isEmpty() && c.getContainsList().isEmpty()) {
                        dispatcher.handleValidationError(new ResultStatus(
                                true, "QueryConfigurationsRequest.criteria.NameCriterion must specify at least one of: exact, prefix, contains"));
                        return;
                    }
                }
                case CATEGORYCRITERION -> {
                    if (criterion.getCategoryCriterion().getValuesList().isEmpty()) {
                        dispatcher.handleValidationError(new ResultStatus(
                                true, "QueryConfigurationsRequest.criteria.CategoryCriterion must specify at least one value"));
                        return;
                    }
                }
                case PARENTCRITERION -> {
                    if (criterion.getParentCriterion().getValuesList().isEmpty()) {
                        dispatcher.handleValidationError(new ResultStatus(
                                true, "QueryConfigurationsRequest.criteria.ParentCriterion must specify at least one value"));
                        return;
                    }
                }
                case TAGSCRITERION -> {
                    if (criterion.getTagsCriterion().getValuesList().isEmpty()) {
                        dispatcher.handleValidationError(new ResultStatus(
                                true, "QueryConfigurationsRequest.criteria.TagsCriterion must specify at least one value"));
                        return;
                    }
                }
                case ATTRIBUTESCRITERION -> {
                    if (criterion.getAttributesCriterion().getKey().isBlank()) {
                        dispatcher.handleValidationError(new ResultStatus(
                                true, "QueryConfigurationsRequest.criteria.AttributesCriterion key must be specified"));
                        return;
                    }
                }
                case CRITERION_NOT_SET -> {
                    dispatcher.handleValidationError(new ResultStatus(
                            true, "QueryConfigurationsRequest.criteria criterion case not set"));
                    return;
                }
            }
        }

        final ConfigurationQueryResult queryResult = mongoClient.executeQueryConfigurations(request);
        if (queryResult == null) {
            dispatcher.handleError("error executing configurations query");
            return;
        }
        dispatcher.handleResult(queryResult);
    }
}
