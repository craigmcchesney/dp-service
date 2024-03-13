package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryMetadataRequest;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import org.bson.Document;

import java.util.Collection;

public interface MongoQueryClientInterface {

    boolean init();
    boolean fini();

    MongoCursor<BucketDocument> executeQueryData(QueryDataRequest.QuerySpec querySpec);

    MongoCursor<Document> executeQueryMetadata(QueryMetadataRequest.QuerySpec querySpec);

    MongoCursor<Document> executeQueryMetadata(Collection<String> pvNameList);

    MongoCursor<Document> executeQueryMetadata(String pvNamePatternString);

}
