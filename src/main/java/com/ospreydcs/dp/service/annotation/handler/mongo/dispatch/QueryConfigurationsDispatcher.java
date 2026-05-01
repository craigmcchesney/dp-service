package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationsResponse;
import com.ospreydcs.dp.grpc.v1.common.Configuration;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.ConfigurationQueryResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;

public class QueryConfigurationsDispatcher extends Dispatcher {

    private final StreamObserver<QueryConfigurationsResponse> responseObserver;
    private final QueryConfigurationsRequest request;

    public QueryConfigurationsDispatcher(
            StreamObserver<QueryConfigurationsResponse> responseObserver,
            QueryConfigurationsRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendQueryConfigurationsResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendQueryConfigurationsResponseError(errorMsg, responseObserver);
    }

    public void handleResult(ConfigurationQueryResult queryResult) {
        final List<Configuration> configurationList = new ArrayList<>();
        for (ConfigurationDocument document : queryResult.getDocuments()) {
            configurationList.add(document.toConfiguration());
        }

        final QueryConfigurationsResponse.QueryConfigurationsResult result =
                QueryConfigurationsResponse.QueryConfigurationsResult.newBuilder()
                        .addAllConfigurations(configurationList)
                        .setNextPageToken(queryResult.getNextPageToken() != null
                                ? queryResult.getNextPageToken() : "")
                        .build();

        AnnotationServiceImpl.sendQueryConfigurationsResponse(result, responseObserver);
    }
}
