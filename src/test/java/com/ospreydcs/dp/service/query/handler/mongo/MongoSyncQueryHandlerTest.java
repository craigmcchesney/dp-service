package com.ospreydcs.dp.service.query.handler.mongo;

import com.mongodb.client.result.InsertManyResult;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class MongoSyncQueryHandlerTest extends MongoQueryHandlerTestBase {

    protected static class TestSyncClient extends MongoSyncQueryClient implements TestClientInterface {

        @Override
        protected String getCollectionNameBuckets() {
            return getTestCollectionNameBuckets();
        }

        @Override
        protected String getCollectionNameRequestStatus() {
            return getTestCollectionNameRequestStatus();
        }

        public int insertBucketDocuments(List<BucketDocument> documentList) {
            InsertManyResult result = mongoCollectionBuckets.insertMany(documentList);
            return result.getInsertedIds().size();
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {

        // Use test db client to set database name globally to "dp-test" and remove that database if it already exists
        MongoTestClient.prepareTestDatabase();

        TestSyncClient testClient = new TestSyncClient();
        MongoQueryHandler handler = new MongoQueryHandler(testClient);
        setUp(handler, testClient);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        MongoQueryHandlerTestBase.tearDown();
    }

    @Test
    public void testResponseStreamDispatcher() {
        super.testResponseStreamDispatcher();
    }

    @Test
    public void testResponseCursorDispatcher() {
        super.testResponseCursorDispatcher();
    }
}
