package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationDocument;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

public class MongoSyncClient extends MongoClientBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    protected MongoClient mongoClient = null;
    protected MongoDatabase mongoDatabase = null;
    protected MongoCollection<ProviderDocument> mongoCollectionProviders = null;
    protected MongoCollection<BucketDocument> mongoCollectionBuckets = null;
    protected MongoCollection<RequestStatusDocument> mongoCollectionRequestStatus = null;
    protected MongoCollection<DataSetDocument> mongoCollectionDataSets = null;
    protected MongoCollection<AnnotationDocument> mongoCollectionAnnotations = null;
    protected MongoCollection<CalculationsDocument> mongoCollectionCalculations = null;
    protected MongoCollection<PvMetadataDocument> mongoCollectionPvMetadata = null;
    protected MongoCollection<ConfigurationDocument> mongoCollectionConfigurations = null;
    protected MongoCollection<ConfigurationActivationDocument> mongoCollectionConfigurationActivations = null;

    @Override
    protected boolean initMongoClient(String connectString) {
        mongoClient = MongoClients.create(connectString);
        return true;
    }

    @Override
    protected boolean initMongoDatabase(String databaseName, CodecRegistry codecRegistry) {

        // run 'hello' to detect mongo topology
        final MongoDatabase adminDatabase = mongoClient.getDatabase(ADMIN_DATABASE_NAME);
        final Document hello = adminDatabase.runCommand(new Document("hello", 1));
        logger.debug("mongo topology detection response: " + hello.toJson());

        if (hello.containsKey("setName")) {
            // Replica set detected
            logger.info("mongo replica set topology detected: " + hello.getString("setName"));

            // Use primary preferred read and majority write for safety
            mongoDatabase = mongoClient.getDatabase(databaseName)
                    .withReadPreference(ReadPreference.primaryPreferred())
                    .withWriteConcern(WriteConcern.MAJORITY);
        } else {
            // Standalone detected
            System.out.println("mongo standalone topology detected");

            // Standalone: normal read/write
            mongoDatabase = mongoClient.getDatabase(databaseName)
                    .withReadPreference(ReadPreference.primary())
                    .withWriteConcern(WriteConcern.ACKNOWLEDGED);
        }

        mongoDatabase = mongoDatabase.withCodecRegistry(codecRegistry);

        return true;
    }

    @Override
    protected boolean initMongoCollectionProviders(String collectionName) {
        mongoCollectionProviders = mongoDatabase.getCollection(collectionName, ProviderDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexProviders(Bson fieldNamesBson) {
        mongoCollectionProviders.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionBuckets(String collectionName) {
        mongoCollectionBuckets = mongoDatabase.getCollection(collectionName, BucketDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexBuckets(Bson fieldNamesBson) {
        mongoCollectionBuckets.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionRequestStatus(String collectionName) {
        mongoCollectionRequestStatus = mongoDatabase.getCollection(collectionName, RequestStatusDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexRequestStatus(Bson fieldNamesBson) {
        mongoCollectionRequestStatus.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionDataSets(String collectionName) {
        mongoCollectionDataSets = mongoDatabase.getCollection(collectionName, DataSetDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexDataSets(Bson fieldNamesBson) {
        mongoCollectionDataSets.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionAnnotations(String collectionName) {
        mongoCollectionAnnotations = mongoDatabase.getCollection(collectionName, AnnotationDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexAnnotations(Bson fieldNamesBson) {
        mongoCollectionAnnotations.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionCalculations(String collectionName) {
        mongoCollectionCalculations = mongoDatabase.getCollection(collectionName, CalculationsDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexCalculations(Bson fieldNamesBson) {
        mongoCollectionCalculations.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionPvMetadata(String collectionName) {
        mongoCollectionPvMetadata = mongoDatabase.getCollection(collectionName, PvMetadataDocument.class);
        return true;
    }

    @Override
    protected boolean createMongoIndexPvMetadata(Bson fieldNamesBson) {
        mongoCollectionPvMetadata.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean createMongoIndexPvMetadataWithOptions(Bson fieldNamesBson, com.mongodb.client.model.IndexOptions indexOptions) {
        mongoCollectionPvMetadata.createIndex(fieldNamesBson, indexOptions);
        return true;
    }

    @Override
    protected boolean initMongoCollectionConfigurations(String collectionName) {
        mongoCollectionConfigurations = mongoDatabase.getCollection(collectionName, ConfigurationDocument.class);
        return true;
    }

    @Override
    protected boolean createMongoIndexConfigurations(Bson fieldNamesBson) {
        mongoCollectionConfigurations.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean createMongoIndexConfigurationsWithOptions(Bson fieldNamesBson, com.mongodb.client.model.IndexOptions indexOptions) {
        mongoCollectionConfigurations.createIndex(fieldNamesBson, indexOptions);
        return true;
    }

    @Override
    protected boolean initMongoCollectionConfigurationActivations(String collectionName) {
        mongoCollectionConfigurationActivations = mongoDatabase.getCollection(collectionName, ConfigurationActivationDocument.class);
        return true;
    }

    @Override
    protected boolean createMongoIndexConfigurationActivations(Bson fieldNamesBson) {
        mongoCollectionConfigurationActivations.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean createMongoIndexConfigurationActivationsWithOptions(Bson fieldNamesBson, com.mongodb.client.model.IndexOptions indexOptions) {
        mongoCollectionConfigurationActivations.createIndex(fieldNamesBson, indexOptions);
        return true;
    }

}
