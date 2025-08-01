package com.ospreydcs.dp.service.ingestionstream.handler.model;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.ingestionstream.handler.IngestionStreamHandler;
import com.ospreydcs.dp.service.ingestionstream.handler.job.EventMonitorSubscribeDataResponseJob;
import com.ospreydcs.dp.service.ingestionstream.handler.monitor.EventMonitor;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class EventMonitorSubscribeDataResponseObserver implements StreamObserver<SubscribeDataResponse> {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final EventMonitor eventMonitor;
    private final IngestionStreamHandler handler;
    private final CountDownLatch ackLatch = new CountDownLatch(1);
    private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
    private final AtomicBoolean isError = new AtomicBoolean(false);


    public EventMonitorSubscribeDataResponseObserver(
            final EventMonitor eventMonitor,
            final IngestionStreamHandler handler
    ) {
        this.eventMonitor = eventMonitor;
        this.handler = handler;
    }

    public boolean awaitAckLatch() {
        boolean await = true;
        try {
            await = ackLatch.await(1, TimeUnit.MINUTES);
            if (!await) {
                final String errorMsg = "timed out waiting for ackLatch";
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
            }
        } catch (InterruptedException e) {
            final String errorMsg = "InterruptedException waiting for ackLatch";
            System.err.println(errorMsg);
            isError.set(true);
            errorMessageList.add(errorMsg);
        }
        return await;
    }

    public boolean isError() {
        return isError.get();
    }

    public String getErrorMessage() {
        if (!errorMessageList.isEmpty()) {
            return errorMessageList.get(0);
        } else {
            return "";
        }
    }

    @Override
    public void onNext(
            SubscribeDataResponse subscribeDataResponse
    ) {
        logger.debug(
                "received SubscribeDataResponse type: {} id: {}",
                subscribeDataResponse.getResultCase().name(),
                this.hashCode());

        switch (subscribeDataResponse.getResultCase()) {
            case EXCEPTIONALRESULT -> {
                isError.set(true);
                errorMessageList.add(subscribeDataResponse.getExceptionalResult().getMessage());
//                if (subscribeDataResponse.getExceptionalResult().getExceptionalResultStatus() == ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT) {
//                    isReject.set(true);
//                } else {
//                    isError.set(true);
//                }
            }
            case ACKRESULT -> {
            }
            case SUBSCRIBEDATARESULT -> {
            }
            case RESULT_NOT_SET -> {
            }
        }

        // decrement ackLatch for initial response
        ackLatch.countDown();

        // dispatch response to subscription manager for handling
        final EventMonitorSubscribeDataResponseJob job =
                new EventMonitorSubscribeDataResponseJob(eventMonitor, subscribeDataResponse);
        handler.addJob(job);
    }

    @Override
    public void onError(Throwable throwable) {
        logger.debug(
                "onError unexpected grpc error for id: {} msg: {}",
                this.hashCode(),
                throwable.getMessage());
        final EventMonitorSubscribeDataResponseJob job =
                new EventMonitorSubscribeDataResponseJob(eventMonitor, true, false);
        handler.addJob(job);
    }

    @Override
    public void onCompleted() {
        logger.debug(
                "onCompleted() response stream closed for id: {}",
                this.hashCode());
        final EventMonitorSubscribeDataResponseJob job =
                new EventMonitorSubscribeDataResponseJob(eventMonitor, false, true);
        handler.addJob(job);
    }
}
