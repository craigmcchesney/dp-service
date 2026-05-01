package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.GetActiveConfigurationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.GetActiveConfigurationsResponse;
import com.ospreydcs.dp.grpc.v1.common.ConfigurationActivation;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.ConfigurationActivationQueryResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;

public class GetActiveConfigurationsDispatcher extends Dispatcher {

    private final StreamObserver<GetActiveConfigurationsResponse> responseObserver;
    private final GetActiveConfigurationsRequest request;

    public GetActiveConfigurationsDispatcher(
            StreamObserver<GetActiveConfigurationsResponse> responseObserver,
            GetActiveConfigurationsRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendGetActiveConfigurationsResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendGetActiveConfigurationsResponseError(errorMsg, responseObserver);
    }

    public void handleResult(ConfigurationActivationQueryResult queryResult) {
        final List<ConfigurationActivation> activationList = new ArrayList<>();
        for (ConfigurationActivationDocument document : queryResult.getDocuments()) {
            activationList.add(document.toConfigurationActivation());
        }

        final GetActiveConfigurationsResponse.GetActiveConfigurationsResult result =
                GetActiveConfigurationsResponse.GetActiveConfigurationsResult.newBuilder()
                        .addAllConfigurationActivations(activationList)
                        .build();

        AnnotationServiceImpl.sendGetActiveConfigurationsResponse(result, responseObserver);
    }
}
