package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import javax.xml.crypto.Data;

public class MongoAsyncClient extends MongoClientBase {

    // static variables
    private static final Logger LOGGER = LogManager.getLogger();

    // instance variables
    protected MongoClient mongoClient = null;
    protected MongoDatabase mongoDatabase = null;
    protected MongoCollection<ProviderDocument> mongoCollectionProviders = null;
    protected MongoCollection<BucketDocument> mongoCollectionBuckets = null;
    protected MongoCollection<RequestStatusDocument> mongoCollectionRequestStatus = null;
    protected MongoCollection<DataSetDocument> mongoCollectionDataSets = null;
    protected MongoCollection<AnnotationDocument> mongoCollectionAnnotations = null;
    protected MongoCollection<CalculationsDocument> mongoCollectionCalculations = null;

    @Override
    protected boolean initMongoClient(String connectString) {
        mongoClient = MongoClients.create(connectString);
        return true;
    }

    @Override
    protected boolean initMongoDatabase(String databaseName, CodecRegistry codecRegistry) {
        mongoDatabase = mongoClient.getDatabase(databaseName);
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
        mongoCollectionRequestStatus =
                mongoDatabase.getCollection(collectionName, RequestStatusDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexRequestStatus(Bson fieldNamesBson) {
        mongoCollectionRequestStatus.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionDataSets(String collectionName) {
        mongoCollectionDataSets =
                mongoDatabase.getCollection(collectionName, DataSetDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexDataSets(Bson fieldNamesBson) {
        mongoCollectionDataSets.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionAnnotations(String collectionName) {
        mongoCollectionAnnotations =
                mongoDatabase.getCollection(collectionName, AnnotationDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexAnnotations(Bson fieldNamesBson) {
        mongoCollectionAnnotations.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionCalculations(String collectionName) {
        mongoCollectionCalculations =
                mongoDatabase.getCollection(collectionName, CalculationsDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexCalculations(Bson fieldNamesBson) {
        mongoCollectionCalculations.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionPvMetadata(String collectionName) {
        // pvMetadata collection not used by async client
        return true;
    }

    @Override
    protected boolean createMongoIndexPvMetadata(Bson fieldNamesBson) {
        // pvMetadata indexes not used by async client
        return true;
    }

    @Override
    protected boolean createMongoIndexPvMetadataWithOptions(Bson fieldNamesBson, com.mongodb.client.model.IndexOptions indexOptions) {
        // pvMetadata indexes not used by async client
        return true;
    }

    @Override
    protected boolean initMongoCollectionConfigurations(String collectionName) {
        // configurations collection not used by async client
        return true;
    }

    @Override
    protected boolean createMongoIndexConfigurations(Bson fieldNamesBson) {
        // configurations indexes not used by async client
        return true;
    }

    @Override
    protected boolean createMongoIndexConfigurationsWithOptions(Bson fieldNamesBson, com.mongodb.client.model.IndexOptions indexOptions) {
        // configurations indexes not used by async client
        return true;
    }

    @Override
    protected boolean initMongoCollectionConfigurationActivations(String collectionName) {
        // configurationActivations collection not used by async client
        return true;
    }

    @Override
    protected boolean createMongoIndexConfigurationActivations(Bson fieldNamesBson) {
        // configurationActivations indexes not used by async client
        return true;
    }

    @Override
    protected boolean createMongoIndexConfigurationActivationsWithOptions(Bson fieldNamesBson, com.mongodb.client.model.IndexOptions indexOptions) {
        // configurationActivations indexes not used by async client
        return true;
    }
}
