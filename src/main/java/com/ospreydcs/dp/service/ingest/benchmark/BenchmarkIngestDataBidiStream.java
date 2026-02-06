package com.ospreydcs.dp.service.ingest.benchmark;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.ingestion.DpIngestionServiceGrpc;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataResponse;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;

public class BenchmarkIngestDataBidiStream extends IngestionBenchmarkBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    /**
     * Implements Callable interface for an executor service task that submits a stream
     * of ingestion requests of specified dimensions,
     * with one request per second for specified number of seconds.
     */
    protected static class BidiStreamingIngestionTask extends IngestionTask {

        public BidiStreamingIngestionTask(
                IngestionTaskParams params,
                IngestDataRequest.IngestionDataFrame.Builder templateDataFrameBuilder,
                Channel channel) {

            super(params, templateDataFrameBuilder, channel);
        }

        public IngestionTaskResult call() {
            IngestionTaskResult result = sendBidiStreamingIngestionRequest(
                    this.params, this.templdateDataFrameBuilder, this.channel);
            return result;
        }

        protected void onResponse(IngestDataResponse response) {
            // hook for subclasses to add validation, default is to do nothing so we don't slow down the benchmark
        }

        /**
         * Invokes streamingIngestion gRPC API with request dimensions and properties
         * as specified in the params.
         * @param params
         * @return
         */
        private IngestionTaskResult sendBidiStreamingIngestionRequest(
                IngestionTaskParams params,
                IngestDataRequest.IngestionDataFrame.Builder templateDataTable,
                Channel channel
        ) {
            final int streamNumber = params.streamNumber;
            final int numSeconds = params.numSeconds;
            final int numRows = params.numRows;
            final int numColumns = params.numColumns;

            final CountDownLatch finishLatch = new CountDownLatch(1);
            final CountDownLatch responseLatch = new CountDownLatch(numSeconds);
            final boolean[] responseError = {false}; // must be final for access by inner class, but we need to modify the value, so final array
            final boolean[] runtimeError = {false}; // must be final for access by inner class, but we need to modify the value, so final array

            /**
             * Implements StreamObserver interface for API response stream.
             */
            StreamObserver<IngestDataResponse> responseObserver = new StreamObserver<IngestDataResponse>() {

                /**
                 * Handles an IngestionResponse object in the API response stream.  Checks properties
                 * of response are as expected.
                 * @param response
                 */
                @Override
                public void onNext(IngestDataResponse response) {

                    responseLatch.countDown();

                    if (!response.hasAckResult()) {
                        // unexpected response
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
                        return;

                    } else {

                        int rowCount = response.getAckResult().getNumRows();
                        int colCount = response.getAckResult().getNumColumns();
                        String requestId = response.getClientRequestId();
                        logger.trace("stream: {} received response for requestId: {}", streamNumber, requestId);

                        if (rowCount != numRows) {
                            logger.error(
                                    "stream: {} response rowCount: {} doesn't match expected rowCount: {}",
                                    streamNumber, rowCount, numRows);
                            responseError[0] = true;
                            finishLatch.countDown();
                            return;

                        }
                        if (colCount != numColumns) {
                            logger.error(
                                    "stream: {} response colCount: {} doesn't match expected colCount: {}",
                                    streamNumber, colCount, numColumns);
                            responseError[0] = true;
                            finishLatch.countDown();
                            return;
                        }

                        // call hook for subclasses to add validation
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

                /**
                 * Handles error in API response stream.  Logs error message and terminates stream.
                 * @param t
                 */
                @Override
                public void onError(Throwable t) {
                    Status status = Status.fromThrowable(t);
                    logger.error("stream: {} ingestDataBidiStream() Failed status: {} message: {}",
                            streamNumber, status, t.getMessage());
                    runtimeError[0] = true;
                    finishLatch.countDown();
                }

                /**
                 * Handles completion of API response stream.  Logs message and terminates stream.
                 */
                @Override
                public void onCompleted() {
                    logger.trace("stream: {} Finished ingestDataBidiStream()", streamNumber);
                    finishLatch.countDown();
                }
            };

            final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub = DpIngestionServiceGrpc.newStub(channel);
            final StreamObserver<IngestDataRequest> requestObserver = asyncStub.ingestDataBidiStream(responseObserver);
            return sendRequestStream(
                    this, requestObserver, finishLatch, responseLatch, responseError, runtimeError);
        }
    }

    protected BidiStreamingIngestionTask newIngestionTask(
            IngestionTaskParams params, IngestDataRequest.IngestionDataFrame.Builder templateDataTable, Channel channel
    ) {
        return new BidiStreamingIngestionTask(params, templateDataTable, channel);
    }

    public static void main(final String[] args) {
        BenchmarkIngestDataBidiStream benchmark = new BenchmarkIngestDataBidiStream();
        
        // Parse command line argument for column data type
        ColumnDataType columnDataType = ColumnDataType.DATA_COLUMN; // Default to legacy DataColumn
//        ColumnDataType columnDataType = ColumnDataType.DOUBLE_COLUMN; // Default to DoubleColumn

        if (args.length > 0) {
            switch (args[0].toLowerCase()) {
                case "--double-column" -> columnDataType = ColumnDataType.DOUBLE_COLUMN;
                case "--serialized-column" -> columnDataType = ColumnDataType.SERIALIZED_DATA_COLUMN;
                case "--data-column" -> columnDataType = ColumnDataType.DATA_COLUMN;
                default -> {
                    System.err.println("Usage: BenchmarkIngestDataBidiStream [--data-column|--double-column|--serialized-column]");
                    System.err.println("  --data-column      Use legacy DataColumn/DataValue structure (default)");
                    System.err.println("  --double-column    Use new efficient DoubleColumn structure");
                    System.err.println("  --serialized-column Use SerializedDataColumn structure");
                    System.exit(1);
                }
            }
        }
        
        System.out.println("Running BenchmarkIngestDataBidiStream with " + columnDataType);
        runBenchmark(benchmark, columnDataType);
    }

}
