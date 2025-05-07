package com.ospreydcs.dp.service.ingest.benchmark;

import com.ospreydcs.dp.service.common.benchmark.BenchmarkMongoClient;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.common.model.BenchmarkScenarioResult;
import com.ospreydcs.dp.service.ingest.utility.RegisterProviderUtility;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.DecimalFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

public abstract class IngestionBenchmarkBase {

    private static final Logger logger = LogManager.getLogger();

    // constants
    protected static final Integer AWAIT_TIMEOUT_MINUTES = 1;
    protected static final Integer TERMINATION_TIMEOUT_MINUTES = 5;
    public static final String NAME_COLUMN_BASE = "dpTest_";

    // configuration
    public static final String BENCHMARK_GRPC_CONNECT_STRING = "localhost:60051";
    public static final String CFG_KEY_START_SECONDS = "IngestionBenchmark.startSeconds";
    public static final Long DEFAULT_START_SECONDS = 1698767462L;

    /**
     * Defines dimensions and properties for IngestionRequest objects to send in an invocation
     * of the streamingIngestion gRPC API.
     */
    protected static class IngestionTaskParams {

        final public long startSeconds;
        final public int streamNumber;
        final public int numSeconds;
        final public int numColumns;
        final public int numRows;
        final public int firstColumnIndex;
        final public int lastColumnIndex;
        final public String providerId;
        final public boolean useTimestampList;
        final public boolean useSerializedDataColumns;

        public IngestionTaskParams(
                long startSeconds,
                int streamNumber,
                int numSeconds,
                int numColumns,
                int numRows,
                int firstColumnIndex,
                int lastColumnIndex,
                String providerId,
                boolean useTimestampList,
                boolean useSerializedDataColumns
        ) {
            this.startSeconds = startSeconds;
            this.streamNumber = streamNumber;
            this.numSeconds = numSeconds;
            this.numColumns = numColumns;
            this.numRows = numRows;
            this.firstColumnIndex = firstColumnIndex;
            this.lastColumnIndex = lastColumnIndex;
            this.providerId = providerId;
            this.useTimestampList = useTimestampList;
            this.useSerializedDataColumns = useSerializedDataColumns;
        }
    }

    /**
     * Encapsulates stats for an invocation of the streamingIngestion API. Includes boolean status
     * and details about data values and bytes sent in the stream.
     */
    protected static class IngestionTaskResult {

        protected boolean status;
        protected long dataValuesSubmitted = 0;
        protected long dataBytesSubmitted = 0;
        protected long grpcBytesSubmitted = 0;

        public boolean getStatus() {
            return status;
        }

        public void setStatus(boolean status) {
            this.status = status;
        }

        public long getDataValuesSubmitted() {
            return dataValuesSubmitted;
        }

        public void setDataValuesSubmitted(long dataValuesSubmitted) {
            this.dataValuesSubmitted = dataValuesSubmitted;
        }

        public long getDataBytesSubmitted() {
            return dataBytesSubmitted;
        }

        public void setDataBytesSubmitted(long dataBytesSubmitted) {
            this.dataBytesSubmitted = dataBytesSubmitted;
        }

        public long getGrpcBytesSubmitted() {
            return grpcBytesSubmitted;
        }

        public void setGrpcBytesSubmitted(long grpcBytesSubmitted) {
            this.grpcBytesSubmitted = grpcBytesSubmitted;
        }

    }

    /**
     * Implements Callable interface for an executor service task that submits a stream
     * of ingestion requests of specified dimensions,
     * with one request per second for specified number of seconds.
     */
    protected static abstract class IngestionTask implements Callable<IngestionTaskResult> {

        protected final IngestionTaskParams params;
        protected final IngestDataRequest.IngestionDataFrame.Builder templdateDataFrameBuilder;
        protected final Channel channel;

        public IngestionTask(
                IngestionTaskParams params,
                IngestDataRequest.IngestionDataFrame.Builder templdateDataFrameBuilder,
                Channel channel) {

            this.params = params;
            this.templdateDataFrameBuilder = templdateDataFrameBuilder;
            this.channel = channel;
        }

        /**
         *
         * @return
         * @throws Exception
         */
        public abstract IngestionTaskResult call();

        protected void onRequest(IngestDataRequest request) {
            // hook for subclasses to add validation, default is to do nothing so we don't slow down the benchmark
        }

        protected void onCompleted() {
            // hook for subclasses to add validation, default is to do nothing so we don't slow down the benchmark
        }

    }

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    /**
     * Generates IngestionRequest API object for specified parameters.
     *
     * @param params
     * @return
     */
    private static IngestDataRequest.IngestionDataFrame.Builder buildDataTableTemplate(IngestionTaskParams params) {

        IngestDataRequest.IngestionDataFrame.Builder dataTableBuilder =
                IngestDataRequest.IngestionDataFrame.newBuilder();

        // build list of Data objects (columns), each a list of Datum objects (cell values)
        final List<DataColumn> dataColumnList = new ArrayList<>();
        final List<SerializedDataColumn> serializedDataColumnList = new ArrayList<>();
        for (int colIndex = params.firstColumnIndex; colIndex <= params.lastColumnIndex ; colIndex++) {
            DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
            dataColumnBuilder.setName(NAME_COLUMN_BASE + colIndex);
            for (int rowIndex = 0 ; rowIndex < params.numRows ; rowIndex++) {
                double cellValue = rowIndex + (double) rowIndex /params.numRows;
                DataValue dataValue = DataValue.newBuilder().setDoubleValue(cellValue).build();
                dataColumnBuilder.addDataValues(dataValue);
//                // use this commented code to look at serialized size of double data
//                int datumSize = rowDatum.getSerializedSize();
//                LOGGER.info("serialized double size: {}", datumSize);
            }
            DataColumn dataColumn = dataColumnBuilder.build();
            if (params.useSerializedDataColumns) {
                final SerializedDataColumn serializedDataColumn =
                        SerializedDataColumn.newBuilder()
                                .setName(dataColumn.getName())
                                .setDataColumnBytes(dataColumn.toByteString())
                                .build();
                serializedDataColumnList.add(serializedDataColumn);
            } else {
                dataColumnList.add(dataColumn);
            }
        }

        if (params.useSerializedDataColumns) {
            dataTableBuilder.setSerializedDataColumns(
                    SerializedDataColumnList.newBuilder()
                            .addAllSerializedDataColumns(serializedDataColumnList)
                            .build());
        } else {
            dataTableBuilder.setDataColumns(
                    DataColumnList.newBuilder().
                            addAllDataColumns(dataColumnList)
                            .build());
        }

        return dataTableBuilder;
    }

    protected static IngestDataRequest prepareIngestionRequest(
            IngestDataRequest.IngestionDataFrame.Builder dataFrameBuilder,
            IngestionTaskParams params,
            Integer secondsOffset
    ) {
        final String providerId = params.providerId;
        final String requestId = String.valueOf(secondsOffset);

        final IngestDataRequest.Builder requestBuilder = IngestDataRequest.newBuilder();

        requestBuilder.setProviderId(providerId);
        requestBuilder.setClientRequestId(requestId);

        // build DataTimestamps for request
        final DataTimestamps.Builder dataTimestampsBuilder = DataTimestamps.newBuilder();
        if (params.useTimestampList) {
            // use TimestampList

            final TimestampList.Builder timestampListBuilder = TimestampList.newBuilder();

             for (int i = 0 ; i < params.numRows ; i++) {
                Timestamp.Builder timestampBuilder = Timestamp.newBuilder();
                timestampBuilder.setEpochSeconds(params.startSeconds + secondsOffset);
                long nanos = i * 1_000_000L;
                timestampBuilder.setNanoseconds(nanos);
                timestampBuilder.build();
                 timestampListBuilder.addTimestamps(timestampBuilder);
             }

             timestampListBuilder.build();
             dataTimestampsBuilder.setTimestampList(timestampListBuilder);

        } else {
            // use SamplingClock

            final Timestamp.Builder startTimestampBuilder = Timestamp.newBuilder();
            startTimestampBuilder.setEpochSeconds(params.startSeconds + secondsOffset);
            startTimestampBuilder.setNanoseconds(0);
            startTimestampBuilder.build();

            final SamplingClock.Builder samplingClockBuilder = SamplingClock.newBuilder();
            samplingClockBuilder.setStartTime(startTimestampBuilder);
            samplingClockBuilder.setPeriodNanos(1_000_000L);
            samplingClockBuilder.setCount(params.numRows);
            samplingClockBuilder.build();

            dataTimestampsBuilder.setSamplingClock(samplingClockBuilder);
        }
        dataTimestampsBuilder.build();
        dataFrameBuilder.setDataTimestamps(dataTimestampsBuilder);


        // add some attributes and event metadata
        final EventMetadata.Builder eventMetadataBuilder = EventMetadata.newBuilder();
        eventMetadataBuilder.setDescription("calibration test");

        final Timestamp.Builder eventTimeBuilder = Timestamp.newBuilder();
        eventTimeBuilder.setEpochSeconds(params.startSeconds);
        eventTimeBuilder.setNanoseconds(0);
        eventTimeBuilder.build();

        eventMetadataBuilder.setStartTimestamp(eventTimeBuilder);
        eventMetadataBuilder.build();
        requestBuilder.setEventMetadata(eventMetadataBuilder);

        final Attribute.Builder subsystemAttributeBuilder = Attribute.newBuilder();
        subsystemAttributeBuilder.setName("subsystem");
        subsystemAttributeBuilder.setValue("vacuum");
        subsystemAttributeBuilder.build();
        requestBuilder.addAttributes(subsystemAttributeBuilder);

        final Attribute.Builder sectorAttributeBuilder = Attribute.newBuilder();
        sectorAttributeBuilder.setName("sector");
        sectorAttributeBuilder.setValue("07");
        sectorAttributeBuilder.build();
        requestBuilder.addAttributes(sectorAttributeBuilder);

        dataFrameBuilder.build();
        requestBuilder.setIngestionDataFrame(dataFrameBuilder);
        return requestBuilder.build();
    }

    protected static IngestionTaskResult sendRequestStream(
            IngestionTask task,
            StreamObserver<IngestDataRequest> requestObserver,
            CountDownLatch finishLatch,
            CountDownLatch responseLatch,
            boolean[] responseError,
            boolean[] runtimeError
    ) {
        final IngestionTaskParams params = task.params;
        final int streamNumber = params.streamNumber;
        final int numSeconds = params.numSeconds;
        final int numRows = params.numRows;
        final int numColumns = params.numColumns;

        final IngestionTaskResult result = new IngestionTaskResult();

        long dataValuesSubmitted = 0;
        long dataBytesSubmitted = 0;
        long grpcBytesSubmitted = 0;
        boolean isError = false;
        try {
            for (int secondsOffset = 0; secondsOffset < numSeconds; secondsOffset++) {

                final String requestId = String.valueOf(secondsOffset);

                // build IngestionRequest for current second, record elapsed time so we can subtract from measurement
                // final IngestionRequest request = buildIngestionRequest(secondsOffset, params);
                final IngestDataRequest request = prepareIngestionRequest(
                        task.templdateDataFrameBuilder, params, secondsOffset);

                // call hook for subclasses to add validation
                try {
                    task.onRequest(request);
                } catch (AssertionError assertionError) {
                    System.err.println("stream: " + streamNumber + " assertion error");
                    assertionError.printStackTrace(System.err);
                    isError = true;
                    break;
                }

                // send grpc ingestion request
                logger.trace("stream: {} sending secondsOffset: {}", streamNumber, secondsOffset);
                requestObserver.onNext(request);

                dataValuesSubmitted = dataValuesSubmitted + (numRows * numColumns);
                dataBytesSubmitted = dataBytesSubmitted + (numRows * numColumns * Double.BYTES);
//                grpcBytesSubmitted = grpcBytesSubmitted + request.getSerializedSize(); // adds 2% performance overhead

                if (finishLatch.getCount() == 0) {
                    // RPC completed or errored before we finished sending.
                    // Sending further requests won't error, but they will just be thrown away.
                    isError = true;
                    break;
                }
            }
        } catch (RuntimeException e) {
            logger.error("stream: {} streamingIngestion() failed: {}", streamNumber, e.getMessage());
            // cancel rpc, onError() sets runtimeError[0]
            isError = true;
        }

        // mark the end of requests
        requestObserver.onCompleted();

        // don't wait for responses if there was already an error
        if (!isError) {
            try {
                // wait until all responses received
                boolean awaitSuccess = responseLatch.await(AWAIT_TIMEOUT_MINUTES, TimeUnit.MINUTES);
                if (!awaitSuccess) {
                    logger.error("stream: {} timeout waiting for responseLatch", streamNumber);
                    result.setStatus(false);
                    return result;
                }
            } catch (InterruptedException e) {
                logger.error(
                        "stream: {} streamingIngestion InterruptedException waiting for responseLatch",
                        streamNumber);
                result.setStatus(false);
                return result;
            }
        }

        // receiving happens asynchronously
        try {
            boolean awaitSuccess = finishLatch.await(AWAIT_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            if (!awaitSuccess) {
                logger.error("stream: {} timeout waiting for finishLatch", streamNumber);
                result.setStatus(false);
                return result;
            }
        } catch (InterruptedException e) {
            logger.error(
                    "stream: {} streamingIngestion InterruptedException waiting for finishLatch",
                    streamNumber);
            result.setStatus(false);
            return result;
        }

        if (responseError[0]) {
            System.err.println("stream: " + streamNumber + " response error encountered");
            result.setStatus(false);
            return result;

        } else if (runtimeError[0]) {
            System.err.println("stream: " + streamNumber + " runtime error encountered");
            result.setStatus(false);
            return result;

        } else if (isError) {
            System.err.println("stream: " + streamNumber + " request error encountered");
            result.setStatus(false);
            return result;

        } else {

            try {
                // call hook for subclasses to add validation
                task.onCompleted();
            } catch (AssertionError assertionError) {
                System.err.println("stream: " + streamNumber + " assertion error");
                assertionError.printStackTrace(System.err);
                result.setStatus(false);
                return result;
            }

//            LOGGER.info("stream: {} responseCount: {}", streamNumber, responseCount);
            result.setStatus(true);
            result.setDataValuesSubmitted(dataValuesSubmitted);
            result.setDataBytesSubmitted(dataBytesSubmitted);
            result.setGrpcBytesSubmitted(grpcBytesSubmitted);
            return result;
        }

    }

    protected abstract IngestionTask newIngestionTask(
            IngestionTaskParams params,
            IngestDataRequest.IngestionDataFrame.Builder templateDataTable,
            Channel channel);

    private String registerProvider(String providerName, Channel channel) {

        // build register provider params
        final RegisterProviderUtility.RegisterProviderRequestParams params
                = new RegisterProviderUtility.RegisterProviderRequestParams(providerName, null);

        // build register provider request
        final RegisterProviderRequest request = RegisterProviderUtility.buildRegisterProviderRequest(params);

        // create response observer
        final RegisterProviderUtility.RegisterProviderResponseObserver responseObserver =
                new RegisterProviderUtility.RegisterProviderResponseObserver();

        // send api request
        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(channel);
        asyncStub.registerProvider(request, responseObserver);

        // wait for response
        responseObserver.await();
        if (responseObserver.isError()) {
            logger.error(
                    "error registering provider: {} message: {}",
                    providerName,
                    responseObserver.getErrorMessage());
            System.exit(1);
        }

        if (responseObserver.getResponseList().size() != 1) {
            logger.error(
                    "unexpected provder registration: {} responseList size: {}",
                    providerName,
                    responseObserver.getResponseList().size());
            System.exit(1);
        }

        final RegisterProviderResponse response = responseObserver.getResponseList().get(0);
        return response.getRegistrationResult().getProviderId();
    }

    /**
     * Executes a multithreaded streaming ingestion scenario with specified properties.
     * Creates an executor service with a fixed size thread pool, and submits a list of
     * IngestionRequestStreamTasks for execution, each of which will call the streamingIngestion API
     * with a list of IngestionRequests.  Calculates and displays scenario performance stats.
     */
    public BenchmarkScenarioResult ingestionScenario(
            Channel channel,
            int numThreads,
            int numStreams,
            int numRows,
            int numColumns,
            int numSeconds,
            boolean generateTimestampListRequests,
            boolean useSerializedDataColumns
    ) {
        boolean success = true;
        long dataValuesSubmitted = 0;
        long dataBytesSubmitted = 0;
        long grpcBytesSubmitted = 0;

        // create thread pool of specified size
        logger.trace("creating thread pool of size: {}", numThreads);
        var executorService = Executors.newFixedThreadPool(numThreads);

        // create list of thread pool tasks, each to submit a stream of IngestionRequests
        // final long startSeconds = Instant.now().getEpochSecond();
        final long startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
        logger.trace("using startSeconds: {}", startSeconds);
        List<IngestionTask> taskList = new ArrayList<>();
        int lastColumnIndex = 0;
        for (int i = 1 ; i <= numStreams ; i++) {

            // register provider for stream number
            final String providerId = registerProvider(String.valueOf(i), channel);

            final int firstColumnIndex = lastColumnIndex + 1;
            lastColumnIndex = lastColumnIndex + numColumns;

            boolean useTimestampList = false;
            // create some requests with explicit timestamp list for sample data generator (not regular benchmark)
            if (i == numStreams) {
                // use TimestampList requests for the last request stream
                if (generateTimestampListRequests) {
                    useTimestampList = true;
                    logger.info(
                            "using DataTimestamps.TimestampList for provider: {} pv index first: {} last: {}",
                            providerId,
                            firstColumnIndex,
                            lastColumnIndex);
                }
            }

            IngestionTaskParams params = new IngestionTaskParams(
                    startSeconds,
                    i,
                    numSeconds,
                    numColumns,
                    numRows,
                    firstColumnIndex,
                    lastColumnIndex,
                    providerId,
                    useTimestampList,
                    useSerializedDataColumns);
            IngestDataRequest.IngestionDataFrame.Builder templateDataTable = buildDataTableTemplate(params);
            IngestionTask task = newIngestionTask(params, templateDataTable, channel);
            taskList.add(task);

        }

        // start performance measurment timer
        Instant t0 = Instant.now();

        // submit tasks to executor service, to send stream of IngestionRequests for each
        List<Future<IngestionTaskResult>> resultList = null;
        try {
            resultList = executorService.invokeAll(taskList);
            executorService.shutdown();
            if (executorService.awaitTermination(TERMINATION_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                for (int i = 0 ; i < resultList.size() ; i++) {
                    Future<IngestionTaskResult> future = resultList.get(i);
                    IngestionTaskResult ingestionResult = future.get();
                    if (!ingestionResult.getStatus()) {
                        success = false;
                        System.err.println("ingestion task failed");
                    }
                    dataValuesSubmitted = dataValuesSubmitted + ingestionResult.getDataValuesSubmitted();
                    dataBytesSubmitted = dataBytesSubmitted + ingestionResult.getDataBytesSubmitted();
                    grpcBytesSubmitted = grpcBytesSubmitted + ingestionResult.getGrpcBytesSubmitted();
                }
            } else {
                logger.error("timeout reached in executorService.awaitTermination");
                executorService.shutdownNow();
            }
        } catch (InterruptedException | ExecutionException ex) {
            executorService.shutdownNow();
            logger.error("Data transmission Interrupted by exception: {}", ex.getMessage());
            Thread.currentThread().interrupt();
        }

        if (success) {

            // stop performance measurement timer, measure elapsed time and subtract time spent building requests
            Instant t1 = Instant.now();
            long dtMillis = t0.until(t1, ChronoUnit.MILLIS);
            double secondsElapsed = dtMillis / 1_000.0;

            String dataValuesSubmittedString = String.format("%,8d", dataValuesSubmitted);
            String dataBytesSubmittedString = String.format("%,8d", dataBytesSubmitted);
            String grpcBytesSubmittedString = String.format("%,8d", grpcBytesSubmitted);
            String grpcOverheadBytesString = String.format("%,8d", grpcBytesSubmitted - dataBytesSubmitted);
            logger.trace("ingestion scenario: {} data values submitted: {}", this.hashCode(), dataValuesSubmittedString);
            logger.trace("ingestion scenario: {} data bytes submitted: {}", this.hashCode(), dataBytesSubmittedString);
            logger.trace("ingestion scenario: {} grpc bytes submitted: {}", this.hashCode(), grpcBytesSubmittedString);
            logger.trace("ingestion scenario: {} grpc overhead bytes: {}", this.hashCode(), grpcOverheadBytesString);

            double dataValueRate = dataValuesSubmitted / secondsElapsed;
            double dataMByteRate = (dataBytesSubmitted / 1_000_000.0) / secondsElapsed;
            double grpcMByteRate = (grpcBytesSubmitted / 1_000_000.0) / secondsElapsed;
            DecimalFormat formatter = new DecimalFormat("#,###.00");
            String dtSecondsString = formatter.format(secondsElapsed);
            String dataValueRateString = formatter.format(dataValueRate);
            String dataMbyteRateString = formatter.format(dataMByteRate);
            String grpcMbyteRateString = formatter.format(grpcMByteRate);
            logger.debug("ingestion scenario: {} execution time: {} seconds", this.hashCode(), dtSecondsString);
            logger.debug("ingestion scenario: {} data value rate: {} values/sec", this.hashCode(), dataValueRateString);
            logger.debug("ingestion scenario: {} data byte rate: {} MB/sec", this.hashCode(), dataMbyteRateString);
            logger.debug("ingestion scenario: {} grpc byte rate: {} MB/sec", this.hashCode(), grpcMbyteRateString);

            return new BenchmarkScenarioResult(true, dataValueRate);

        } else {
            System.err.println("streaming ingestion scenario failed, performance data invalid");
            return new BenchmarkScenarioResult(false, 0.0);
        }
    }

    protected void ingestionExperiment(Channel channel, boolean useSerializedDataColumns) {

        // number of PVS, sampling rate, length of run time
        // one minute of data at 4000 PVs x 1000 samples per second for 60 seconds
        final int numPvs = 4000;
        final int samplesPerSecond = 1000;
        final int numSeconds = 60;


        // set up arrays of parameters to sweep
        final int[] numThreadsArray = {1, 3, 5, 7};
        final int[] numStreamsArray = {20, 50, 75, 100};
        Map<String, Double> writeRateMap = new TreeMap<>();
        for (int numThreads : numThreadsArray) {
            for (int numStreams : numStreamsArray) {

                String mapKey = "numThreads: " + numThreads + " numStreams: " + numStreams;

                // dimensions for each stream - smaller number of streams means bigger grpc messages
                final int numColumns = numPvs / numStreams;
                final int numRows = samplesPerSecond;

                logger.info("running streaming ingestion scenario, numThreads: {} numStreams: {}",
                        numThreads, numStreams);

                // empty and initialize benchmark database
                BenchmarkMongoClient.prepareBenchmarkDatabase();

                BenchmarkScenarioResult scenarioResult =
                        ingestionScenario(
                                channel,
                                numThreads,
                                numStreams,
                                numRows,
                                numColumns,
                                numSeconds,
                                false,
                                useSerializedDataColumns);
                if (scenarioResult.success) {
                    writeRateMap.put(mapKey, scenarioResult.valuesPerSecond);
                } else {
                    System.err.println("error running scenario");
                    return;
                }
            }
        }

        // print results summary
        double maxRate = 0.0;
        double minRate = 100_000_000;
        System.out.println("======================================");
        System.out.println("Streaming Ingestion Experiment Results");
        System.out.println("======================================");
        final DecimalFormat formatter = new DecimalFormat("#,###.00");
        for (var mapEntry : writeRateMap.entrySet()) {
            final String mapKey = mapEntry.getKey();
            final double writeRate = mapEntry.getValue();
            final String dataValueRateString = formatter.format(writeRate);
            System.out.println(mapKey + " writeRate: " + dataValueRateString + " values/sec");
            if (writeRate > maxRate) {
                maxRate = writeRate;
            }
            if (writeRate < minRate) {
                minRate = writeRate;
            }
        }
        System.out.println("max write rate: " + maxRate);
        System.out.println("min write rate: " + minRate);
    }

    public static void runBenchmark(IngestionBenchmarkBase benchmark, boolean useSerializedDataColumns) {

        final String connectString = BENCHMARK_GRPC_CONNECT_STRING;
        logger.info("Creating gRPC channel using connect string: {}", connectString);
        final ManagedChannel channel =
                Grpc.newChannelBuilder(connectString, InsecureChannelCredentials.create()).build();

        benchmark.ingestionExperiment(channel, useSerializedDataColumns);

        try {
            boolean awaitSuccess = channel.shutdownNow().awaitTermination(TERMINATION_TIMEOUT_MINUTES, TimeUnit.SECONDS);
            if (!awaitSuccess) {
                logger.error("timeout in channel.shutdownNow.awaitTermination");
            }
        } catch (InterruptedException e) {
            logger.error("InterruptedException in channel.shutdownNow.awaitTermination: " + e.getMessage());
        }
    }

}
