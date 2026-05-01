package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.DeleteConfigurationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.DeleteConfigurationResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.MongoDeleteResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

public class DeleteConfigurationDispatcher extends Dispatcher {

    private final StreamObserver<DeleteConfigurationResponse> responseObserver;
    private final DeleteConfigurationRequest request;

    public DeleteConfigurationDispatcher(
            StreamObserver<DeleteConfigurationResponse> responseObserver,
            DeleteConfigurationRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendDeleteConfigurationResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleResult(MongoDeleteResult result) {
        if (result.isError) {
            AnnotationServiceImpl.sendDeleteConfigurationResponseError(result.message, responseObserver);
        } else if (result.deletedPvName == null) {
            final String msg = "no Configuration record found for: " + request.getConfigurationName();
            AnnotationServiceImpl.sendDeleteConfigurationResponseReject(msg, responseObserver);
        } else {
            AnnotationServiceImpl.sendDeleteConfigurationResponseSuccess(result.deletedPvName, responseObserver);
        }
    }
}
