package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.DeleteConfigurationActivationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.DeleteConfigurationActivationResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.MongoDeleteResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

public class DeleteConfigurationActivationDispatcher extends Dispatcher {

    private final StreamObserver<DeleteConfigurationActivationResponse> responseObserver;
    private final DeleteConfigurationActivationRequest request;

    public DeleteConfigurationActivationDispatcher(
            StreamObserver<DeleteConfigurationActivationResponse> responseObserver,
            DeleteConfigurationActivationRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendDeleteConfigurationActivationResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleResult(MongoDeleteResult result) {
        if (result.isError) {
            AnnotationServiceImpl.sendDeleteConfigurationActivationResponseError(result.message, responseObserver);
        } else if (result.deletedPvName == null) {
            final String keyDescription = switch (request.getKeyCase()) {
                case CLIENTACTIVATIONID -> "clientActivationId: " + request.getClientActivationId();
                case COMPOSITEKEY -> "configurationName: " + request.getCompositeKey().getConfigurationName();
                default -> "unknown key";
            };
            final String msg = "no ConfigurationActivation record found for: " + keyDescription;
            AnnotationServiceImpl.sendDeleteConfigurationActivationResponseReject(msg, responseObserver);
        } else {
            AnnotationServiceImpl.sendDeleteConfigurationActivationResponseSuccess(result.deletedPvName, responseObserver);
        }
    }
}
