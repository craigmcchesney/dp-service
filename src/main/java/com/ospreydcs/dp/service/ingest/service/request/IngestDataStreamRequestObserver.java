package com.ospreydcs.dp.service.ingest.service.request;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataResponse;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataStreamResponse;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.ingest.handler.IngestionValidationUtility;
import com.ospreydcs.dp.service.ingest.handler.interfaces.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IngestDataStreamRequestObserver extends IngestionStreamRequestObserverBase {

    private final StreamObserver<IngestDataStreamResponse> responseObserver;
    private final List<String> requestIdList = Collections.synchronizedList(new ArrayList<>());
    private final List<String> rejectedIdList = Collections.synchronizedList(new ArrayList<>());

    public IngestDataStreamRequestObserver(
            StreamObserver<IngestDataStreamResponse> responseObserver,
            IngestionHandlerInterface handler,
            IngestionServiceImpl serviceImpl
    ) {
        super(handler, serviceImpl);
        this.responseObserver = responseObserver;
    }

    @Override
    protected void handleIngestionRequest_(IngestDataRequest request) {

        if (handler.getShutdownRequested()) {
            final IngestDataStreamResponse rejectResponse =
                    IngestionServiceImpl.ingestDataStreamResponseStreamReject(request, "service is shutdown");
            responseObserver.onNext(rejectResponse);
            responseObserver.onCompleted();
            return;
        }

        this.requestIdList.add(request.getClientRequestId());

        // validate request, send error response for invalid request
        final ResultStatus resultStatus = IngestionValidationUtility.validateIngestionRequest(request);
        boolean validationError = false;
        String validationMsg = "";

        if (resultStatus.isError) {
            validationError = true;
            validationMsg = resultStatus.msg;
            this.rejectedIdList.add(request.getClientRequestId());

        }

        // handle the request, even if rejected (to update request status database)
        final HandlerIngestionRequest handlerIngestionRequest =
                new HandlerIngestionRequest(request, validationError, validationMsg);
        handler.handleIngestionRequest(handlerIngestionRequest);
    }

    @Override
    protected void handleClose_() {

        // generate response, either ExceptionalResult if any rejects, otherwise IngestDataStreamResult.
        serviceImpl.sendIngestDataStreamResponse(responseObserver, requestIdList, rejectedIdList);

        // close response stream
        responseObserver.onCompleted();
    }
}
