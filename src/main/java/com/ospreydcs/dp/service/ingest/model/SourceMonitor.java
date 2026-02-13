package com.ospreydcs.dp.service.ingest.model;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataFrame;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.ingest.handler.interfaces.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import com.ospreydcs.dp.service.ingestionstream.service.IngestionStreamServiceImpl;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SourceMonitor {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final IngestionHandlerInterface handler;
    public final List<String> pvNames;
    public final StreamObserver<SubscribeDataResponse> responseObserver;
    public final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    public SourceMonitor(
            IngestionHandlerInterface handler,
            List<String> pvNames,
            StreamObserver<SubscribeDataResponse> responseObserver
    ) {
        this.handler = handler;
        this.pvNames = pvNames;
        this.responseObserver = responseObserver;
    }

    public void publishDataFrame(
            final String pvName,
            DataFrame dataFrame
    ) {
        if (!safeToSendResponse()) {
            return;
        }

        logger.debug(
                "publishing DataFrame for id: {} pv: {}",
                responseObserver.hashCode(),
                pvName);
        IngestionServiceImpl.sendSubscribeDataResponse(
                dataFrame, responseObserver);
    }

    public void handleReject(String errorMsg) {

        logger.debug(
                "handleReject id: {} msg: {}",
                responseObserver.hashCode(),
                errorMsg);

        if (!safeToSendResponse()) {
            return;
        }

        // dispatch error message but don't close response stream with onCompleted()
        IngestionServiceImpl.sendSubscribeDataResponseReject(errorMsg, responseObserver);
    }

    public void handleError(String errorMsg) {

        logger.debug(
                "handleError id: {} msg: {}",
                responseObserver.hashCode(),
                errorMsg);

        if (!safeToSendResponse()) {
            return;
        }

        // dispatch error message but don't close response stream with onCompleted()
        IngestionServiceImpl.sendSubscribeDataResponseError(errorMsg, responseObserver);
    }

    private boolean safeToSendResponse() {
        if (shutdownRequested.get()) {
            return false;
        }
        
        ServerCallStreamObserver<SubscribeDataResponse> serverCallStreamObserver =
                (ServerCallStreamObserver<SubscribeDataResponse>) responseObserver;
        return !serverCallStreamObserver.isCancelled();
    }

    public void requestShutdown() {

        logger.debug("requestShutdown id: {}", responseObserver.hashCode());

        // use AtomicBoolean flag to control cancel, we only need one caller thread cleaning things up
        if (shutdownRequested.compareAndSet(false, true)) {

            // close API response stream
            ServerCallStreamObserver<SubscribeDataResponse> serverCallStreamObserver =
                    (ServerCallStreamObserver<SubscribeDataResponse>) responseObserver;
            if (!serverCallStreamObserver.isCancelled()) {
                logger.debug(
                        "SourceMonitor.close() calling responseObserver.onCompleted id: {}",
                        responseObserver.hashCode());
                responseObserver.onCompleted();
            } else {
                logger.debug(
                        "SourceMonitor.close() responseObserver already closed id: {}",
                        responseObserver.hashCode());
            }
        }
    }
}
