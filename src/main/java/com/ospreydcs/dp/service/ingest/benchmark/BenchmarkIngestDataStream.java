package com.ospreydcs.dp.service.ingest.benchmark;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.ingestion.DpIngestionServiceGrpc;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataStreamResponse;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;

public class BenchmarkIngestDataStream extends IngestionBenchmarkBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    protected static class UniStreamingIngestionTask extends IngestionTask {

        public UniStreamingIngestionTask(
                IngestionTaskParams params,
                IngestDataRequest.IngestionDataFrame.Builder templateDataFrameBuilder,
                Channel channel
        ) {
            super(params, templateDataFrameBuilder, channel);
        }

        public IngestionTaskResult call() {
            IngestionTaskResult result = sendUniStreamingIngestionRequest(
                    this.params, this.templdateDataFrameBuilder, this.channel);
            return result;
        }

        protected void onResponse(IngestDataStreamResponse response) {
            // hook for subclasses to add validation, default is to do nothing so we don't slow down the benchmark
        }

        private IngestionTaskResult sendUniStreamingIngestionRequest(
                IngestionTaskParams params,
                IngestDataRequest.IngestionDataFrame.Builder templateDataTable,
                Channel channel
        ) {
            final int streamNumber = params.streamNumber;
            final int numRequests = params.numSeconds;

            final CountDownLatch finishLatch = new CountDownLatch(1);
            final CountDownLatch responseLatch = new CountDownLatch(1);
            final boolean[] responseError = {false}; // must be final for access by inner class, but we need to modify the value, so final array
            final boolean[] runtimeError = {false}; // must be final for access by inner class, but we need to modify the value, so final array

            StreamObserver<IngestDataStreamResponse> responseObserver = new StreamObserver<>() {

                @Override
                public void onNext(
                        IngestDataStreamResponse response
                ) {
                    responseLatch.countDown();

                    if (response.hasExceptionalResult()) {

                        if (response.getExceptionalResult().getExceptionalResultStatus()
                                == ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT) {
                            logger.error("received reject with msg: "
                                    + response.getExceptionalResult().getMessage());
                        } else {
                            logger.error("unexpected responseType: "
                                    + response.getExceptionalResult().getExceptionalResultStatus().getDescriptorForType());
                        }

                        responseError[0] = true;
                        finishLatch.countDown();

                    } else if (response.hasIngestDataStreamResult()) {
                        IngestDataStreamResponse.IngestDataStreamResult result = response.getIngestDataStreamResult();

                        logger.trace(
                                "stream: {} received response for {} requests",
                                streamNumber,
                                result.getNumRequests());

                        if (result.getNumRequests() != numRequests) {
                            logger.error("stream: {} numRequests: {} mismatch expected: {}",
                                    streamNumber,
                                    result.getNumRequests(),
                                    numRequests);
                            responseError[0] = true;
                            finishLatch.countDown();
                            return;
                        }

                        try {
                            onResponse(response);
                        } catch (AssertionError assertionError) {
                            if (finishLatch.getCount() != 0) {
                                System.err.println("stream: " + streamNumber + " assertion error");
                                assertionError.printStackTrace(System.err);
                                finishLatch.countDown();
                            }
                            responseError[0] = true;
                            return;
                        }
                    }
                }

                @Override
                public void onError(
                        Throwable t
                ) {
                    Status status = Status.fromThrowable(t);
                    logger.error("stream: {} ingestDataUniStream() Failed status: {} message: {}",
                            streamNumber, status, t.getMessage());
                    runtimeError[0] = true;
                    finishLatch.countDown();
                }

                @Override
                public void onCompleted(
                ) {
                    logger.trace("stream: {} Finished ingestDataUniStream()", streamNumber);
                    finishLatch.countDown();
                }
            };

            final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub = DpIngestionServiceGrpc.newStub(channel);
            StreamObserver<IngestDataRequest> requestObserver = asyncStub.ingestDataStream(responseObserver);
            return sendRequestStream(
                    this, requestObserver, finishLatch, responseLatch, responseError, runtimeError);
        }
    }

    protected UniStreamingIngestionTask newIngestionTask(
            IngestionTaskParams params, IngestDataRequest.IngestionDataFrame.Builder templateDataTable, Channel channel
    ) {
        return new UniStreamingIngestionTask(params, templateDataTable, channel);
    }

    public static void main(final String[] args) {
        BenchmarkIngestDataStream benchmark = new BenchmarkIngestDataStream();
        
        // Parse command line argument for column data type
        ColumnDataType columnDataType = ColumnDataType.DATA_COLUMN; // Default to legacy DataColumn
        
        if (args.length > 0) {
            switch (args[0].toLowerCase()) {
                case "--double-column" -> columnDataType = ColumnDataType.DOUBLE_COLUMN;
                case "--serialized-column" -> columnDataType = ColumnDataType.SERIALIZED_DATA_COLUMN;
                case "--data-column" -> columnDataType = ColumnDataType.DATA_COLUMN;
                default -> {
                    System.err.println("Usage: BenchmarkIngestDataStream [--data-column|--double-column|--serialized-column]");
                    System.err.println("  --data-column      Use legacy DataColumn/DataValue structure (default)");
                    System.err.println("  --double-column    Use new efficient DoubleColumn structure");
                    System.err.println("  --serialized-column Use SerializedDataColumn structure");
                    System.exit(1);
                }
            }
        }
        
        System.out.println("Running BenchmarkIngestDataStream with " + columnDataType);
        runBenchmark(benchmark, columnDataType);
    }

}
