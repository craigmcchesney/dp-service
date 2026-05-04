package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationDocument;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

public class MongoTestClient extends MongoSyncClient {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // constants
    public static final String MONGO_TEST_DATABASE_NAME = "dp-test";
    public static final int MONGO_FIND_RETRY_COUNT = 300;
    public static final int MONGO_FIND_RETRY_INTERVAL_MILLIS = 100;

    @Override
    public boolean init() {

        // override the default database name globally
        logger.info("overriding db name globally to: {}", MONGO_TEST_DATABASE_NAME);
        MongoClientBase.setMongoDatabaseName(MONGO_TEST_DATABASE_NAME);

        // init so we have database client for dropping existing db
        super.init();
        dropTestDatabase();
        super.fini();

        // re-initialize to recreate db and collections as needed
        return super.init();
    }

    public void dropTestDatabase() {
        logger.info("dropping database: {}", MONGO_TEST_DATABASE_NAME);
        MongoDatabase database = this.mongoClient.getDatabase(MONGO_TEST_DATABASE_NAME);
        database.drop();
    }

    public static void prepareTestDatabase() {
        MongoTestClient testClient = new MongoTestClient();
        testClient.init();
    }

    public ProviderDocument findProvider(String providerId) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            List<ProviderDocument> matchingDocuments = new ArrayList<>();
            mongoCollectionProviders.find(eq("_id", new ObjectId(providerId))).into(matchingDocuments);
            if (matchingDocuments.size() > 0) {
                return matchingDocuments.get(0);
            } else {
                try {
                    logger.info("findProvider id: " + providerId + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public BucketDocument findBucket(String id) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            List<BucketDocument> matchingBuckets = new ArrayList<>();
            mongoCollectionBuckets.find(eq("_id", id)).into(matchingBuckets);
            if (matchingBuckets.size() > 0) {
                return matchingBuckets.get(0);
            } else {
                try {
                    logger.info("findBucket id: " + id + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public RequestStatusDocument findRequestStatus(String providerId, String requestId) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount) {
            List<RequestStatusDocument> matchingDocuments = new ArrayList<>();
            Bson filter = and(eq("providerId", providerId), eq("requestId", requestId));
            mongoCollectionRequestStatus.find(filter).into(matchingDocuments);
            if (matchingDocuments.size() > 0) {
                return matchingDocuments.get(0);
            } else {
                try {
                    logger.info("findRequestStatus providerId: " + providerId
                            + " requestId: " + requestId
                            + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public DataSetDocument findDataSet(String dataSetId) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            List<DataSetDocument> matchingDocuments = new ArrayList<>();
            mongoCollectionDataSets.find(eq("_id", new ObjectId(dataSetId))).into(matchingDocuments);
            if (matchingDocuments.size() > 0) {
                return matchingDocuments.get(0);
            } else {
                try {
                    logger.info("findDataSet id: " + dataSetId + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public AnnotationDocument findAnnotation(String annotationId) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            final List<AnnotationDocument> matchingAnnotations = new ArrayList<>();
            mongoCollectionAnnotations.find(eq("_id", new ObjectId(annotationId))).into(matchingAnnotations);
            if (matchingAnnotations.size() > 0) {
                return matchingAnnotations.get(0);
            } else {
                try {
                    logger.info("findAnnotation id: " + annotationId + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public PvMetadataDocument findPvMetadata(String pvName) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            final List<PvMetadataDocument> matchingDocuments = new ArrayList<>();
            mongoCollectionPvMetadata.find(eq("pvName", pvName)).into(matchingDocuments);
            if (matchingDocuments.size() > 0) {
                return matchingDocuments.get(0);
            } else {
                try {
                    logger.info("findPvMetadata pvName: " + pvName + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public List<BucketDocument> findDataSetBuckets(DataSetDocument dataset) {
        final MongoSyncQueryClient mongoSyncQueryClient = new MongoSyncQueryClient();
        mongoSyncQueryClient.init();
        final List<BucketDocument> datasetBuckets = new ArrayList<>();
        for (DataBlockDocument dataBlock : dataset.getDataBlocks()) {
            final MongoCursor<BucketDocument> documentCursor = mongoSyncQueryClient.executeDataBlockQuery(dataBlock);
            while (documentCursor.hasNext()) {
                datasetBuckets.add(documentCursor.next());
            }
        }
        return datasetBuckets;
    }

    public MongoCursor<BucketDocument> findDataBlockBuckets(DataBlockDocument dataBlock) {
        final MongoSyncQueryClient mongoSyncQueryClient = new MongoSyncQueryClient();
        mongoSyncQueryClient.init();
        return mongoSyncQueryClient.executeDataBlockQuery(dataBlock);
    }

    public ConfigurationDocument findConfiguration(String configurationName) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            final List<ConfigurationDocument> matchingDocuments = new ArrayList<>();
            mongoCollectionConfigurations.find(eq("configurationName", configurationName)).into(matchingDocuments);
            if (matchingDocuments.size() > 0) {
                return matchingDocuments.get(0);
            } else {
                try {
                    logger.info("findConfiguration configurationName: " + configurationName + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public ConfigurationActivationDocument findConfigurationActivationById(String clientActivationId) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            final List<ConfigurationActivationDocument> matchingDocuments = new ArrayList<>();
            mongoCollectionConfigurationActivations.find(eq("clientActivationId", clientActivationId))
                    .into(matchingDocuments);
            if (matchingDocuments.size() > 0) {
                return matchingDocuments.get(0);
            } else {
                try {
                    logger.info("findConfigurationActivationById id: " + clientActivationId + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public ConfigurationActivationDocument findConfigurationActivationByCompositeKey(
            String configurationName, Instant startTime) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            final List<ConfigurationActivationDocument> matchingDocuments = new ArrayList<>();
            final Bson filter = and(
                    eq("configurationName", configurationName),
                    eq("startTime", startTime));
            mongoCollectionConfigurationActivations.find(filter).into(matchingDocuments);
            if (matchingDocuments.size() > 0) {
                return matchingDocuments.get(0);
            } else {
                try {
                    logger.info("findConfigurationActivationByCompositeKey configurationName: "
                            + configurationName + " startTime: " + startTime + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public CalculationsDocument findCalculations(String calculationsId) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            final List<CalculationsDocument> matchingDocuments = new ArrayList<>();
            mongoCollectionCalculations.find(eq("_id", new ObjectId(calculationsId))).into(matchingDocuments);
            if (matchingDocuments.size() > 0) {
                return matchingDocuments.get(0);
            } else {
                try {
                    logger.info("findCalculations id: " + calculationsId + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

}
