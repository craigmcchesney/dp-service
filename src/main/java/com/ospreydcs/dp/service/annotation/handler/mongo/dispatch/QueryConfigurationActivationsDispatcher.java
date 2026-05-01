package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationActivationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationActivationsResponse;
import com.ospreydcs.dp.grpc.v1.common.ConfigurationActivation;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.ConfigurationActivationQueryResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;

public class QueryConfigurationActivationsDispatcher extends Dispatcher {

    private final StreamObserver<QueryConfigurationActivationsResponse> responseObserver;
    private final QueryConfigurationActivationsRequest request;

    public QueryConfigurationActivationsDispatcher(
            StreamObserver<QueryConfigurationActivationsResponse> responseObserver,
            QueryConfigurationActivationsRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendQueryConfigurationActivationsResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendQueryConfigurationActivationsResponseError(errorMsg, responseObserver);
    }

    public void handleResult(ConfigurationActivationQueryResult queryResult) {
        final List<ConfigurationActivation> activationList = new ArrayList<>();
        for (ConfigurationActivationDocument document : queryResult.getDocuments()) {
            activationList.add(document.toConfigurationActivation());
        }

        final QueryConfigurationActivationsResponse.QueryConfigurationActivationsResult result =
                QueryConfigurationActivationsResponse.QueryConfigurationActivationsResult.newBuilder()
                        .addAllConfigurationActivations(activationList)
                        .setNextPageToken(queryResult.getNextPageToken() != null
                                ? queryResult.getNextPageToken() : "")
                        .build();

        AnnotationServiceImpl.sendQueryConfigurationActivationsResponse(result, responseObserver);
    }
}
