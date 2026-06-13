package com.ospreydcs.dp.service.common.mongo.driver;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertManyResult;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoSyncDriverTest {

    private static MongoClient mongoClient = null;
    private static MongoDatabase mongoDatabase = null;

    @BeforeClass
    public static void setUp() throws Exception {

        System.out.println("setUp");

        // Use test db client to set database name globally to "dp-test" and remove that database if it already exists
        MongoTestClient.prepareTestDatabase();

        mongoClient = MongoClients.create(MongoClientBase.getMongoConnectString());
        mongoDatabase = mongoClient.getDatabase(MongoClientBase.getMongoDatabaseName());
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
            try {
                InsertManyResult insertManyResult = collection.insertMany(batch1);
                assertTrue(
                        "mongodb error inserting batch1, write not acknowledged",
                        insertManyResult.wasAcknowledged());
                int numInsertedIds = insertManyResult.getInsertedIds().size();
                assertTrue(
                        "insertedIds != 1: " + numInsertedIds,
                        numInsertedIds == batch1.size());
            } catch (MongoException ex) {
                fail("mongo exception inserting batch1: " + ex.getMessage());
            }
        }

        {
            // insert batch2 containing duplicate id, check result
            List<Document> batch2 = new ArrayList<>();
            Document doc2 = new Document();
            doc2.put(keyId, valueId);
            batch2.add(doc2);
            boolean isException = false;
            try {
                InsertManyResult insertManyResult = collection.insertMany(batch2);
            } catch (MongoException ex) {
                isException = true;
            }
            assertTrue("no exception encountered inserting batch2", isException);
        }
    }

}
