package com.ospreydcs.dp.service.query.benchmark;

import com.mongodb.client.result.InsertManyResult;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.benchmark.BenchmarkMongoClient;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketUtility;
import com.ospreydcs.dp.service.common.model.BenchmarkScenarioResult;
import com.ospreydcs.dp.service.ingest.benchmark.IngestionBenchmarkBase;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.DecimalFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.*;

public abstract class QueryBenchmarkBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // constants
    protected static final Integer AWAIT_TIMEOUT_MINUTES = 1;
    protected static final Integer TERMINATION_TIMEOUT_MINUTES = 5;
    private static final int NUM_SCENARIO_SECONDS = 60;

    // configuration
    public static final String BENCHMARK_GRPC_CONNECT_STRING = "localhost:60052";

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    protected static class BenchmarkDbClient extends MongoSyncQueryClient {

        public int insertBucketDocuments(List<BucketDocument> documentList) {
            InsertManyResult result = mongoCollectionBuckets.insertMany(documentList);
            return result.getInsertedIds().size();
        }
    }

    static class InsertTaskParams {

        final public BenchmarkDbClient dbClient;
        final public long bucketStartSeconds;
        final public int numSamplesPerSecond;
        final public int numSecondsPerBucket;
        final public int numColumns;

        public InsertTaskParams(
                BenchmarkDbClient dbClient,
                long bucketStartSeconds,
                int numSamplesPerSecond,
                int numSecondsPerBucket,
                int numColumns
        ) {
            this.dbClient = dbClient;
            this.bucketStartSeconds = bucketStartSeconds;
            this.numSamplesPerSecond = numSamplesPerSecond;
            this.numSecondsPerBucket = numSecondsPerBucket;
            this.numColumns = numColumns;
        }
    }

    static class InsertTaskResult {
        public final int bucketsInserted;
        public InsertTaskResult(int bucketsInserted) {
            this.bucketsInserted = bucketsInserted;
        }
    }

    static class InsertTask implements Callable<InsertTaskResult> {

        final public InsertTaskParams params;

        public InsertTask (InsertTaskParams params) {
            this.params = params;
        }

        public InsertTaskResult call() {
            return createAndInsertBucket(params);
        }

    }

    private static InsertTaskResult createAndInsertBucket(InsertTaskParams params) {
        List<BucketDocument> bucketList = BucketUtility.createBucketDocuments(
                params.bucketStartSeconds,
                params.numSamplesPerSecond,
                params.numSecondsPerBucket,
                IngestionBenchmarkBase.NAME_COLUMN_BASE,
                params.numColumns,
                1);
        int bucketsInserted = params.dbClient.insertBucketDocuments(bucketList);
        return new InsertTaskResult(bucketsInserted);
    }

    protected static void loadBucketData(long startSeconds) {

        BenchmarkDbClient dbClient = new BenchmarkDbClient();

        // load database with data for query
        Instant t0 = Instant.now();
        dbClient.init();
        final int numSamplesPerSecond = 1000;
        final int numSecondsPerBucket = 1;
        final int numColumns = 4000;
        final int numBucketsPerColumn = 60;

        // set up executorService with tasks to create and insert a batch of bucket documents
        // with a task for each second's data
        var executorService = Executors.newFixedThreadPool(7);
        List<InsertTask> insertTaskList = new ArrayList<>();
        for (int bucketIndex = 0 ; bucketIndex < numBucketsPerColumn ; ++bucketIndex) {
            InsertTaskParams taskParams = new InsertTaskParams(
                    dbClient,
                    startSeconds+bucketIndex,
                    numSamplesPerSecond,
                    numSecondsPerBucket,
                    numColumns);
            InsertTask task = new InsertTask(taskParams);
            insertTaskList.add(task);
        }

        // invoke tasks to create and insert bucket documents via executorService
        logger.info("loading database, using startSeconds: {}", startSeconds);
        List<Future<InsertTaskResult>> insertTaskResultFutureList = null;
        try {
            insertTaskResultFutureList = executorService.invokeAll(insertTaskList);
            executorService.shutdown();
            if (executorService.awaitTermination(TERMINATION_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                for (int i = 0 ; i < insertTaskResultFutureList.size() ; i++) {
                    Future<InsertTaskResult> future = insertTaskResultFutureList.get(i);
                    InsertTaskResult result = future.get();
                    if (result.bucketsInserted != numColumns) {
                        logger.error("loading error, unexpected numBucketsInserted: {}", result.bucketsInserted);
                        dbClient.fini();
                        System.exit(1);
                    }
                }
            } else {
                logger.error("loading error, executorService.awaitTermination reached timeout");
                executorService.shutdownNow();
                dbClient.fini();
                System.exit(1);
            }
        } catch (InterruptedException | ExecutionException ex) {
            logger.error("loading error, executorService interrupted by exception: {}", ex.getMessage());
            executorService.shutdownNow();
            dbClient.fini();
            Thread.currentThread().interrupt();
            System.exit(1);
        }

        // clean up after loading and calculate stats
        dbClient.fini();
        logger.info("finished loading database");
        Instant t1 = Instant.now();
        long dtMillis = t0.until(t1, ChronoUnit.MILLIS);
        double secondsElapsed = dtMillis / 1_000.0;
        final DecimalFormat formatter = new DecimalFormat("#,###.00");
        String dtSecondsString = formatter.format(secondsElapsed);
        logger.info("loading time: {} seconds", dtSecondsString);
    }

    public record QueryDataRequestTaskParams(
            int streamNumber,
            List<String> columnNames,
            long startSeconds,
            int numSeconds,
            boolean useSerializedDataColumns
    ) {
    }

    protected record QueryTaskResult(
            boolean status,
            long dataValuesReceived,
            long dataBytesReceived,
            long grpcBytesReceived
    ) {
    }

    /**
     * Base class for ExecutorService Callable tasks that send a query and handle the response.
     */
    protected static abstract class QueryTask implements Callable<QueryTaskResult> {

        protected final Channel channel;

        public QueryTask(Channel channel) {
            this.channel = channel;
        }

        public abstract QueryTaskResult call();

        protected void onCompleted() {
            // hook for subclasses to add validation, default is to do nothing so we don't slow down the benchmark
        }

    }


    /**
     * Intermediate base class for query tasks that send a QueryDataRequest to one of the queryData* APIs.
     */
    protected static abstract class QueryDataRequestTask extends QueryTask {

        protected final QueryDataRequestTaskParams params;

        public QueryDataRequestTask(
                Channel channel,
                QueryDataRequestTaskParams params
        ) {
            super(channel);
            this.params = params;
        }

        protected void onRequest(QueryDataRequest request) {
            // hook for subclasses to add validation, default is to do nothing so we don't slow down the benchmark
        }

    }

    /**
     * Intermediate base class for query tasks that receive one or more QueryDataResponse objects.
     */
    protected static abstract class QueryDataResponseTask extends QueryDataRequestTask {

        public QueryDataResponseTask(Channel channel, QueryDataRequestTaskParams params) {
            super(channel, params);
        }

        protected void onResponse(QueryDataResponse response) {
            // hook for subclasses to add validation, default is to do nothing so we don't slow down the benchmark
        }
    }

    protected static QueryDataRequest buildQueryDataRequest(QueryDataRequestTaskParams params) {

        Timestamp.Builder beginTimeBuilder = Timestamp.newBuilder();
        beginTimeBuilder.setEpochSeconds(params.startSeconds);
        beginTimeBuilder.setNanoseconds(0);
        beginTimeBuilder.build();

        final long endSeconds = params.startSeconds + params.numSeconds;
        Timestamp.Builder endTimeBuilder = Timestamp.newBuilder();
        endTimeBuilder.setEpochSeconds(endSeconds);
        endTimeBuilder.setNanoseconds(0);
        endTimeBuilder.build();

        QueryDataRequest.Builder requestBuilder = QueryDataRequest.newBuilder();

        QueryDataRequest.QuerySpec.Builder querySpecBuilder = QueryDataRequest.QuerySpec.newBuilder();
        querySpecBuilder.setBeginTime(beginTimeBuilder);
        querySpecBuilder.setEndTime(endTimeBuilder);
        querySpecBuilder.addAllPvNames(params.columnNames);
        querySpecBuilder.build();
        requestBuilder.setQuerySpec(querySpecBuilder);

        return requestBuilder.build();
    }

    public static QueryDataRequest buildNextQueryDataRequest() {
        QueryDataRequest.Builder requestBuilder = QueryDataRequest.newBuilder();
        QueryDataRequest.CursorOperation.Builder cursorOperationBuilder = QueryDataRequest.CursorOperation.newBuilder();
        cursorOperationBuilder.setCursorOperationType(QueryDataRequest.CursorOperation.CursorOperationType.CURSOR_OP_NEXT);
        cursorOperationBuilder.build();
        requestBuilder.setCursorOp(cursorOperationBuilder);
        return requestBuilder.build();
    }

    protected abstract QueryDataRequestTask newQueryTask(
            Channel channel, QueryDataRequestTaskParams params);

    public BenchmarkScenarioResult queryScenario(
            Channel channel,
            int numPvs,
            int pvsPerRequest,
            int numThreads,
            long startSeconds,
            int numSeconds,
            boolean useSerializedDataColumns
    ) {
        boolean success = true;
        long dataValuesReceived = 0;
        long dataBytesReceived = 0;
        long grpcBytesReceived = 0;

        // create thread pool of specified size
        logger.trace("creating thread pool of size: {}", numThreads);
        final var executorService = Executors.newFixedThreadPool(numThreads);

        // create list of thread pool tasks, each to submit a stream of IngestionRequests
        final List<QueryDataRequestTask> taskList = new ArrayList<>();
        List<String> currentBatchColumns = new ArrayList<>();
        int currentBatchIndex = 1;
        for (int i = 1 ; i <= numPvs ; i++) {
            final String columnName = IngestionBenchmarkBase.NAME_COLUMN_BASE + i;
            currentBatchColumns.add(columnName);
            if (currentBatchColumns.size() == pvsPerRequest) {
                // add task for existing batch of columns
                final QueryDataRequestTaskParams params =
                        new QueryDataRequestTaskParams(
                                currentBatchIndex,
                                currentBatchColumns,
                                startSeconds,
                                numSeconds,
                                useSerializedDataColumns);
                final QueryDataRequestTask task = newQueryTask(channel, params);
                taskList.add(task);
                // start a new batch of columns
                currentBatchColumns = new ArrayList<>();
                currentBatchIndex = currentBatchIndex + 1;
            }
        }
        // add task for final batch of columns, if not empty
        if (!currentBatchColumns.isEmpty()) {
            final QueryDataRequestTaskParams params =
                    new QueryDataRequestTaskParams(
                            currentBatchIndex,
                            currentBatchColumns,
                            startSeconds,
                            numSeconds, false);
            final QueryDataRequestTask task = newQueryTask(channel, params);
            taskList.add(task);
        }

        // start performance measurement timer
        final Instant t0 = Instant.now();

        // submit tasks to executor service
        List<Future<QueryTaskResult>> resultList = null;
        try {
            resultList = executorService.invokeAll(taskList);
            executorService.shutdown();
            if (executorService.awaitTermination(QueryBenchmarkBase.TERMINATION_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                for (int i = 0 ; i < resultList.size() ; i++) {
                    Future<QueryTaskResult> future = resultList.get(i);
                    QueryTaskResult result = future.get();
                    if (!result.status) {
                        success = false;
                    }
                    dataValuesReceived = dataValuesReceived + result.dataValuesReceived;
                    dataBytesReceived = dataBytesReceived + result.dataBytesReceived;
                    grpcBytesReceived = grpcBytesReceived + result.grpcBytesReceived;
                }
                if (!success) {
                    logger.error("thread pool future returned false");
                }
            } else {
                logger.error("timeout reached in executorService.awaitTermination");
                executorService.shutdownNow();
            }
        } catch (InterruptedException | ExecutionException ex) {
            executorService.shutdownNow();
            logger.warn("Data transmission interrupted by exception: {}", ex.getMessage());
            Thread.currentThread().interrupt();
        }

        if (success) {

            // stop performance measurement timer, measure elapsed time and subtract time spent building requests
            final Instant t1 = Instant.now();
            final long dtMillis = t0.until(t1, ChronoUnit.MILLIS);
            final double secondsElapsed = dtMillis / 1_000.0;

            final String dataValuesReceivedString = String.format("%,8d", dataValuesReceived);
            final String dataBytesReceivedString = String.format("%,8d", dataBytesReceived);
            final String grpcBytesReceivedString = String.format("%,8d", grpcBytesReceived);
            final String grpcOverheadBytesString = String.format("%,8d", grpcBytesReceived - dataBytesReceived);
            logger.trace("query scenario: {} data values received: {}", this.hashCode(), dataValuesReceivedString);
            logger.trace("query scenario: {} data bytes received: {}", this.hashCode(), dataBytesReceivedString);
            logger.trace("query scenario: {} grpc bytes received: {}", this.hashCode(), grpcBytesReceivedString);
            logger.trace("query scenario: {} grpc overhead bytes: {}", this.hashCode(), grpcOverheadBytesString);

            final double dataValueRate = dataValuesReceived / secondsElapsed;
            final double dataMByteRate = (dataBytesReceived / 1_000_000.0) / secondsElapsed;
            final double grpcMByteRate = (grpcBytesReceived / 1_000_000.0) / secondsElapsed;
            final DecimalFormat formatter = new DecimalFormat("#,###.00");
            final String dtSecondsString = formatter.format(secondsElapsed);
            final String dataValueRateString = formatter.format(dataValueRate);
            final String dataMbyteRateString = formatter.format(dataMByteRate);
            final String grpcMbyteRateString = formatter.format(grpcMByteRate);
            logger.debug("query scenario: {} execution time: {} seconds", this.hashCode(), dtSecondsString);
            logger.debug("query scenario: {} data value rate: {} values/sec", this.hashCode(), dataValueRateString);
            logger.debug("query scenario: {} data byte rate: {} MB/sec", this.hashCode(), dataMbyteRateString);
            logger.debug("query scenario: {} grpc byte rate: {} MB/sec", this.hashCode(), grpcMbyteRateString);

            return new BenchmarkScenarioResult(true, dataValueRate);

        } else {
            logger.error("scenario failed, performance data invalid");
        }

        return new BenchmarkScenarioResult(false, 0.0);
    }

    protected void queryExperiment(
            Channel channel,
            int[] totalNumPvsArray,
            int[] numPvsPerRequestArray,
            int[] numThreadsArray,
            long startSeconds,
            boolean useSerializedDataColumns
    ) {
//        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final DecimalFormat numPvsFormatter = new DecimalFormat("0000");
        Map<String, Double> rateMap = new TreeMap<>();
        for (int numPvs : totalNumPvsArray) {
            for (int pvsPerRequest : numPvsPerRequestArray) {
                for (int numThreads : numThreadsArray) {
                    String mapKey =
                            "numPvs: " + numPvsFormatter.format(numPvs) + " pvsPerRequest: " + pvsPerRequest + " numThreads: " + numThreads;
                    logger.info(
                            "running queryScenario, numPvs: {} pvsPerRequest: {} threads: {}",
                            numPvs,pvsPerRequest, numThreads);
                    BenchmarkScenarioResult scenarioResult =
                            queryScenario(
                                    channel,
                                    numPvs,
                                    pvsPerRequest,
                                    numThreads,
                                    startSeconds,
                                    NUM_SCENARIO_SECONDS,
                                    useSerializedDataColumns);
                    double writeRate = scenarioResult.valuesPerSecond;
                    rateMap.put(mapKey, writeRate);
                }
            }
        }

        // print results summary
        double maxRate = 0.0;
        double minRate = 100_000_000;
        System.out.println("======================================");
        System.out.println("queryExperiment results");
        System.out.println("======================================");
        final DecimalFormat formatter = new DecimalFormat("#,###.00");
        for (var mapEntry : rateMap.entrySet()) {
            final String mapKey = mapEntry.getKey();
            final double rate = mapEntry.getValue();
            final String dataValueRateString = formatter.format(rate);
            System.out.println(mapKey + " rate: " + dataValueRateString + " values/sec");
            if (rate > maxRate) {
                maxRate = rate;
            }
            if (rate < minRate) {
                minRate = rate;
            }
        }
        System.out.println("max rate: " + formatter.format(maxRate));
        System.out.println("min rate: " + formatter.format(minRate));
    }

    public static void runBenchmark(
            QueryBenchmarkBase benchmark,
            int[] totalNumPvsArray,
            int[] numPvsPerRequestArray,
            int[] numThreadsArray,
            boolean useSerializedDataColumns
    ) {
        long startSeconds = Instant.now().getEpochSecond();

        // load data for use by the query benchmark
        BenchmarkMongoClient.prepareBenchmarkDatabase(); // override default db name to dp-benchmark and initialize db
        loadBucketData(startSeconds);

        // Create a communication channel to the server, known as a Channel. Channels are thread-safe
        // and reusable. It is common to create channels at the beginning of your application and reuse
        // them until the application shuts down.
        //
        // For the example we use plaintext insecure credentials to avoid needing TLS certificates. To
        // use TLS, use TlsChannelCredentials instead.
        String connectString = BENCHMARK_GRPC_CONNECT_STRING;
        logger.info("Creating gRPC channel using connect string: {}", connectString);
        final ManagedChannel channel =
                Grpc.newChannelBuilder(connectString, InsecureChannelCredentials.create()).build();

        benchmark.queryExperiment(
                channel,
                totalNumPvsArray,
                numPvsPerRequestArray,
                numThreadsArray,
                startSeconds,
                useSerializedDataColumns);

        // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
        // resources the channel should be shut down when it will no longer be used. If it may be used
        // again leave it running.
        try {
            boolean awaitSuccess = channel.shutdownNow().awaitTermination(
                    TERMINATION_TIMEOUT_MINUTES, TimeUnit.SECONDS);
            if (!awaitSuccess) {
                logger.error("timeout in channel.shutdownNow.awaitTermination");
            }
        } catch (InterruptedException e) {
            logger.error("InterruptedException in channel.shutdownNow.awaitTermination: " + e.getMessage());
        }

    }

}
