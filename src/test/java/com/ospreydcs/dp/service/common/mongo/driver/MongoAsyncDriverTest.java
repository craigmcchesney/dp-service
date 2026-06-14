package com.ospreydcs.dp.service.common.mongo.driver;

import com.mongodb.MongoException;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.ingest.handler.mongo.ObservableSubscriber;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoAsyncDriverTest {

    private static MongoClient mongoClient = null;
    private static MongoDatabase mongoDatabase = null;

    @BeforeClass
    public static void setUp() throws Exception {

        System.out.println("setUp");

        // Use test db client to set database name globally to "dp-test" and remove that database if it already exists
        MongoTestClient.prepareTestDatabase();

        mongoClient = MongoClients.create(MongoTestClient.getMongoConnectString());
        mongoDatabase = mongoClient.getDatabase(MongoTestClient.getConfiguredTestDatabaseName());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        System.out.println("tearDown");
    }

    @Test
    public void testInsertDuplicateId() {

        System.out.println("test01InsertDuplicateId");

        String collectionName = "InsertDuplicateId-" + System.currentTimeMillis();
        mongoDatabase.createCollection(collectionName);
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);

        String keyId = "_id";
        String valueId = "duplicate";

        {
            // insert batch1 with original id, check result
            List<Document> batch1 = new ArrayList<>();
            Document doc1 = new Document();
            doc1.put(keyId, valueId);
            batch1.add(doc1);
            boolean isException = false;
            try {
                Publisher<InsertManyResult> publisher = collection.insertMany(batch1);
                var subscriber = new ObservableSubscriber<>();
                publisher.subscribe(subscriber);
                subscriber.await();
                var receivedList = subscriber.getReceived();
                int receivedListSize = receivedList.size();
                assertTrue(
                        "recievedList size != 1: " + receivedListSize,
                        receivedListSize == 1);
                InsertManyResult insertManyResult = (InsertManyResult) receivedList.get(0);
                assertTrue(
                        "mongodb error inserting batch1, write not acknowledged",
                        insertManyResult.wasAcknowledged());
                int numInsertedIds = insertManyResult.getInsertedIds().size();
                assertTrue(
                        "insertedIds != 1: " + numInsertedIds,
                        numInsertedIds == batch1.size());
            } catch (MongoException ex) {
                fail("unexpected exception encountered inserting batch1");
            }
        }

        {
            // insert batch2 with duplicate id, check result
            List<Document> batch2 = new ArrayList<>();
            Document doc2 = new Document();
            doc2.put(keyId, valueId);
            batch2.add(doc2);
            boolean isException = false;
            try {
                Publisher<InsertManyResult> publisher = collection.insertMany(batch2);
                var subscriber = new ObservableSubscriber<>();
                publisher.subscribe(subscriber);
                subscriber.await();
                fail("no exception encountered inerting duplicate ids");
            } catch (MongoException ex) {
                isException = true;
            }
            assertTrue("no exception encountered inserting batch2", isException);
        }
    }

}
