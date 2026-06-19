package com.ospreydcs.dp.service.common.benchmark;

import com.mongodb.client.MongoDatabase;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BenchmarkMongoClient extends MongoSyncClient {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // constants
    public static final String BENCHMARK_DATABASE_NAME = "dp-benchmark";
    public static final String CFG_KEY_BENCHMARK_DATABASE_NAME = "MongoClient.benchmarkDatabaseName";

    @Override
    public boolean init() {

        // resolve benchmark database name from config (supports DP_MONGO_BENCHMARK_DB_NAME env override), falling back to constant
        String benchmarkDatabaseName = configMgr().getConfigString(CFG_KEY_BENCHMARK_DATABASE_NAME, BENCHMARK_DATABASE_NAME);

        // override the default database name globally
        if (!benchmarkDatabaseName.equals(BENCHMARK_DATABASE_NAME)) {
            logger.warn("overriding benchmark db name globally to: {} — THIS DATABASE WILL BE DROPPED", benchmarkDatabaseName);
        } else {
            logger.info("overriding db name globally to: {}", benchmarkDatabaseName);
        }
        MongoClientBase.setMongoDatabaseName(benchmarkDatabaseName);

        // init so we have database client for dropping existing db
        super.init();
        dropBenchmarkDatabase();
        super.fini();

        // re-initialize to recreate db and collections as needed
        return super.init();
    }

    public void dropBenchmarkDatabase() {
        String dbName = getMongoDatabaseName();
        if (dbName.equals(MongoClientBase.MONGO_DATABASE_NAME)) {
            throw new IllegalStateException(
                    "dropBenchmarkDatabase() refused to drop production database: " + dbName);
        }
        logger.warn("dropping database: {}", dbName);
        MongoDatabase database = this.mongoClient.getDatabase(dbName);
        database.drop();
    }

    public static void prepareBenchmarkDatabase() {
        BenchmarkMongoClient benchmarkClient = new BenchmarkMongoClient();
        benchmarkClient.init();
    }

}
