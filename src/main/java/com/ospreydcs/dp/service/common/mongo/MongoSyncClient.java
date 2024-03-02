package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

public class MongoSyncClient extends MongoClientBase {

    private static final Logger LOGGER = LogManager.getLogger();

    protected MongoClient mongoClient = null;
    protected MongoDatabase mongoDatabase = null;
    protected MongoCollection<BucketDocument> mongoCollectionBuckets = null;
    protected MongoCollection<RequestStatusDocument> mongoCollectionRequestStatus = null;
    protected MongoCollection<AnnotationDocument> mongoCollectionAnnotations = null;

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
    protected boolean initMongoCollectionAnnotations(String collectionName) {
        mongoCollectionAnnotations = mongoDatabase.getCollection(collectionName, AnnotationDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexAnnotations(Bson fieldNamesBson) {
        mongoCollectionAnnotations.createIndex(fieldNamesBson);
        return true;
    }

}
