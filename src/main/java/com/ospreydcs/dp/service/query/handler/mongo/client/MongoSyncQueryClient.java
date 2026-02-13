package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.PvMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.bson.ProviderMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Indexes.ascending;

public class MongoSyncQueryClient extends MongoSyncClient implements MongoQueryClientInterface {

    private static final Logger logger = LogManager.getLogger();

    public MongoCursor<BucketDocument> executeBucketDocumentQuery(
            Bson columnNameFilter,
            long startTimeSeconds,
            long startTimeNanos,
            long endTimeSeconds,
            long endTimeNanos
    ) {
        final Bson endTimeFilter =
                or(lt(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, endTimeSeconds),
                        and(eq(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, endTimeSeconds),
                                lt(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS, endTimeNanos)));
        final Bson startTimeFilter =
                or(gt(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS, startTimeSeconds),
                        and(eq(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS, startTimeSeconds),
                                gte(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_NANOS, startTimeNanos)));
        final Bson filter = and(columnNameFilter, endTimeFilter, startTimeFilter);

        logger.debug("executing query columns: " + columnNameFilter
                + " startSeconds: " + startTimeSeconds
                + " endSeconds: " + endTimeSeconds);

        return mongoCollectionBuckets
                .find(filter)
                .sort(ascending(
                        BsonConstants.BSON_KEY_PV_NAME,
                        BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                        BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS
                ))
                .cursor();
    }

    @Override
    public MongoCursor<BucketDocument> executeDataBlockQuery(DataBlockDocument dataBlock) {

        final long startTimeSeconds = dataBlock.getBeginTime().getSeconds();
        final long startTimeNanos = dataBlock.getBeginTime().getNanos();
        final long endTimeSeconds = dataBlock.getEndTime().getSeconds();
        final long endTimeNanos = dataBlock.getEndTime().getNanos();

        final Bson columnNameFilter = in(BsonConstants.BSON_KEY_PV_NAME, dataBlock.getPvNames());
        return executeBucketDocumentQuery(
                columnNameFilter, startTimeSeconds, startTimeNanos, endTimeSeconds, endTimeNanos);
    }

    @Override
    public MongoCursor<BucketDocument> executeQueryData(QueryDataRequest.QuerySpec querySpec) {

        // snippet to get query plan
//        Document explanation = collection.find().explain(ExplainVerbosity.EXECUTION_STATS);
//        List<String> keys = Arrays.asList("queryPlanner", "winningPlan");
//        System.out.println(explanation.getEmbedded(keys, Document.class).toJson());

        final long startTimeSeconds = querySpec.getBeginTime().getEpochSeconds();
        final long startTimeNanos = querySpec.getBeginTime().getNanoseconds();
        final long endTimeSeconds = querySpec.getEndTime().getEpochSeconds();
        final long endTimeNanos = querySpec.getEndTime().getNanoseconds();

        final Bson columnNameFilter = in(BsonConstants.BSON_KEY_PV_NAME, querySpec.getPvNamesList());
        return executeBucketDocumentQuery(
                columnNameFilter, startTimeSeconds, startTimeNanos, endTimeSeconds, endTimeNanos);
    }

    @Override
    public MongoCursor<BucketDocument> executeQueryTable(QueryTableRequest request) {
        
        final long startTimeSeconds = request.getBeginTime().getEpochSeconds();
        final long startTimeNanos = request.getBeginTime().getNanoseconds();
        final long endTimeSeconds = request.getEndTime().getEpochSeconds();
        final long endTimeNanos = request.getEndTime().getNanoseconds();

        // create name filter using either list of pv names, or pv name pattern
        Bson columnNameFilter = null;
        switch (request.getPvNameSpecCase()) {
            case PVNAMELIST -> {
                columnNameFilter = in(BsonConstants.BSON_KEY_PV_NAME, request.getPvNameList().getPvNamesList());
            }
            case PVNAMEPATTERN -> {
                final Pattern pvNamePattern = Pattern.compile(
                        request.getPvNamePattern().getPattern(), Pattern.CASE_INSENSITIVE);
                columnNameFilter = Filters.regex(BsonConstants.BSON_KEY_PV_NAME, pvNamePattern);
            }
            case PVNAMESPEC_NOT_SET -> {
                return null;
            }
        }

        // execute query
        return executeBucketDocumentQuery(
                columnNameFilter, startTimeSeconds, startTimeNanos, endTimeSeconds, endTimeNanos);
    }

    private MongoCursor<PvMetadataQueryResultDocument> executeQueryPvMetadata(Bson columnNameFilter) {

        // NOTE: PROJECTION MUST INCLUDE KEYS FOR ALL FIELDS USED IN SORTING and GROUPING!!!
        // If not the values will silently be null and lead to unexpected results!!

        Bson bucketFieldProjection = Projections.fields(Projections.include(
                BsonConstants.BSON_KEY_PV_NAME,
                BsonConstants.BSON_KEY_BUCKET_ID,
                BsonConstants.BSON_KEY_BUCKET_DATA_TYPE,
                BsonConstants.BSON_KEY_BUCKET_DATA_TIMESTAMPS_CASE,
                BsonConstants.BSON_KEY_BUCKET_DATA_TIMESTAMPS_TYPE,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS,
                BsonConstants.BSON_KEY_BUCKET_LAST_TIME,
                BsonConstants.BSON_KEY_BUCKET_SAMPLE_COUNT,
                BsonConstants.BSON_KEY_BUCKET_SAMPLE_PERIOD,
                BsonConstants.BSON_KEY_BUCKET_PROVIDER_ID,
                BsonConstants.BSON_KEY_BUCKET_PROVIDER_NAME
        ));

        // Sort fields must appear in projection.
        Bson bucketSort = ascending(
                BsonConstants.BSON_KEY_PV_NAME,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS);

        Bson metadataSort = ascending(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME);

        logger.debug("executeQueryMetadata query: {}", columnNameFilter.toString());

        var aggregateIterable = mongoCollectionBuckets.withDocumentClass(PvMetadataQueryResultDocument.class)
                .aggregate(
                        Arrays.asList(
                                Aggregates.match(columnNameFilter),
                                Aggregates.project(bucketFieldProjection),
                                Aggregates.sort(bucketSort), // sort buckets here so that records are ordered for group opeator

                                // Bucket fields for grouping must appear in projection!!
                                Aggregates.group(
                                        "$" + BsonConstants.BSON_KEY_PV_NAME,
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_PV_METADATA_PV_NAME,
                                                "$" + BsonConstants.BSON_KEY_PV_NAME),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_BUCKET_ID,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_ID),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_BUCKET_DATA_TYPE,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_DATA_TYPE),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_BUCKET_DATA_TIMESTAMPS_CASE,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_DATA_TIMESTAMPS_CASE),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_BUCKET_DATA_TIMESTAMPS_TYPE,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_DATA_TIMESTAMPS_TYPE),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_BUCKET_SAMPLE_COUNT,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_SAMPLE_COUNT),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_BUCKET_SAMPLE_PERIOD,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_SAMPLE_PERIOD),
                                        Accumulators.first(
                                                // save the first time of the first document in group to the firstTime field
                                                BsonConstants.BSON_KEY_PV_METADATA_FIRST_DATA_TIMESTAMP,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_FIRST_TIME),
                                        Accumulators.last(
                                                // save the last time of the last document to the lastTime field
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_DATA_TIMESTAMP,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_LAST_TIME),
                                        Accumulators.sum(
                                                // count number of bucket documents in group for this pv
                                                BsonConstants.BSON_KEY_PV_METADATA_NUM_BUCKETS,
                                                1),
                                        Accumulators.last(
                                                // save the providerId of the last document to the providerid field
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_PROVIDER_ID,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_PROVIDER_ID),
                                        Accumulators.last(
                                                // save the providerName of the last document to the providerName field
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_PROVIDER_NAME,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_PROVIDER_NAME)
                                ),
                                Aggregates.sort(metadataSort) // sort metadata documents so result is sorted
                                ));

//        aggregateIterable.forEach(bucketDocument -> {System.out.println(bucketDocument.toString());});

        return aggregateIterable.cursor();
    }

    @Override
    public MongoCursor<PvMetadataQueryResultDocument> executeQueryPvMetadata(Collection<String> pvNameList) {
        final Bson pvNameFilter = in(BsonConstants.BSON_KEY_PV_NAME, pvNameList);
        return executeQueryPvMetadata(pvNameFilter);
    }

    @Override
    public MongoCursor<PvMetadataQueryResultDocument> executeQueryPvMetadata(String pvNamePatternString) {
        final Pattern pvNamePattern = Pattern.compile(pvNamePatternString, Pattern.CASE_INSENSITIVE);
        final Bson pvNameFilter = Filters.regex(BsonConstants.BSON_KEY_PV_NAME, pvNamePattern);
        return executeQueryPvMetadata(pvNameFilter);
    }

    @Override
    public MongoCursor<PvMetadataQueryResultDocument> executeQueryPvMetadata(QueryPvMetadataRequest request) {
        if (request.hasPvNameList()) {
            return executeQueryPvMetadata(request.getPvNameList().getPvNamesList());
        } else {
            return executeQueryPvMetadata(request.getPvNamePattern().getPattern());
        }
    }

    @Override
    public MongoCursor<ProviderDocument> executeQueryProviders(QueryProvidersRequest request) {
        
        // create filter to select providers
        final List<Bson> globalFilterList = new ArrayList<>();
        final List<Bson> criteriaFilterList = new ArrayList<>();
        final List<QueryProvidersRequest.Criterion> criterionList = request.getCriteriaList();
        for (QueryProvidersRequest.Criterion criterion : criterionList) {

            switch (criterion.getCriterionCase()) {

                case IDCRITERION -> {
                    // provider id filter, combined with other filters by AND operator
                    final String providerId = criterion.getIdCriterion().getId();
                    if (!providerId.isBlank()) {
                        Bson idFilter = Filters.eq(BsonConstants.BSON_KEY_PROVIDER_ID, new ObjectId(providerId));
                        globalFilterList.add(idFilter);
                    }
                }

                case TEXTCRITERION -> {
                    // name filter, combined with other filters by AND operator
                    final String nameText = criterion.getTextCriterion().getText();
                    if ( ! nameText.isBlank()) {
                        final Bson nameFilter = Filters.text(nameText);
                        globalFilterList.add(nameFilter);
                    }
                }

                case TAGSCRITERION -> {
                    // tags filter, combined with other filters by OR operator
                    final String tagValue = criterion.getTagsCriterion().getTagValue();
                    if ( ! tagValue.isBlank()) {
                        Bson tagsFilter = Filters.in(BsonConstants.BSON_KEY_TAGS, tagValue);
                        criteriaFilterList.add(tagsFilter);
                    }
                }

                case ATTRIBUTESCRITERION -> {
                    // attributes filter, combined with other filters by OR operator
                    final String attributeKey = criterion.getAttributesCriterion().getKey();
                    final String attributeValue = criterion.getAttributesCriterion().getValue();
                    if ( ! attributeKey.isBlank() && ! attributeValue.isBlank()) {
                        final String mapKey = BsonConstants.BSON_KEY_ATTRIBUTES + "." + attributeKey;
                        Bson attributesFilter = Filters.eq(mapKey, attributeValue);
                        criteriaFilterList.add(attributesFilter);
                    }
                }

                case CRITERION_NOT_SET -> {
                    // shouldn't happen since validation checks for this, but...
                    logger.error("executeQueryProviders unexpected error criterion case not set");
                }
            }
        }

        if (globalFilterList.isEmpty() && criteriaFilterList.isEmpty()) {
            // shouldn't happen since validation checks for this, but...
            logger.debug("no search criteria specified in QueryAnnotationsRequest filter");
            return null;
        }

        // create global filter to be combined with and operator (default matches all Annotations)
        Bson globalFilter = Filters.exists(BsonConstants.BSON_KEY_ANNOTATION_ID);
        if (globalFilterList.size() > 0) {
            globalFilter = and(globalFilterList);
        }

        // create criteria filter to be combined with or operator (default matches all Annotations)
        Bson criteriaFilter = Filters.exists(BsonConstants.BSON_KEY_ANNOTATION_ID);
        if (criteriaFilterList.size() > 0) {
            criteriaFilter = or(criteriaFilterList);
        }

        // combine global filter with criteria filter using and operator
        final Bson queryFilter = and(globalFilter, criteriaFilter);
        
        logger.debug("executing queryProviders filter: " + queryFilter.toString());

        final MongoCursor<ProviderDocument> resultCursor = mongoCollectionProviders
                .find(queryFilter)
                .sort(ascending(BsonConstants.BSON_KEY_PROVIDER_NAME))
                .cursor();

        if (resultCursor == null) {
            logger.error("executeQueryProviders received null cursor from mongodb.find");
        }

        return resultCursor;
    }

    @Override
    public MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderMetadata(
            QueryProviderMetadataRequest request
    ) {
        if (request.getProviderId().isBlank()) {
            // this has already been validated but just in case...
            logger.error("executeQueryProviderMetadata unexpected error providerId not specified");
            return null;
        }

        return executeQueryProviderMetadata(request.getProviderId());
    }

    @Override
    public MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderMetadata(String providerid) {

        // generate filter for buckets query by providerId
        final Bson providerIdFilter = eq(BsonConstants.BSON_KEY_BUCKET_PROVIDER_ID, providerid);

        // NOTE: PROJECTION MUST INCLUDE KEYS FOR ALL FIELDS USED IN SORTING and GROUPING!!!
        // If not the values will silently be null and lead to unexpected results!!
        Bson bucketFieldProjection = Projections.fields(Projections.include(
                BsonConstants.BSON_KEY_BUCKET_PROVIDER_ID,
                BsonConstants.BSON_KEY_PV_NAME,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME
        ));

        // Sort fields must appear in projection.  Specifies sorting of documents with specified providerId by bucket firstTime.
        Bson bucketSort = ascending(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME);

        // This is used to sort the result of the final aggregated result.
        Bson metadataSort = ascending(BsonConstants.BSON_KEY_BUCKET_PROVIDER_ID);

        logger.debug("executeQueryProviderMetadata query: {}", providerIdFilter.toString());

        var aggregateIterable = mongoCollectionBuckets.withDocumentClass(ProviderMetadataQueryResultDocument.class)
                .aggregate(
                        Arrays.asList(
                                Aggregates.match(providerIdFilter),
                                Aggregates.project(bucketFieldProjection),
                                Aggregates.sort(bucketSort), // sort buckets here so that records are ordered for group opeator

                                // Bucket fields for grouping must appear in projection!!
                                Aggregates.group(
                                        "$" + BsonConstants.BSON_KEY_BUCKET_PROVIDER_ID,
                                        Accumulators.addToSet(
                                                // collect a set of unique PV names for this provider
                                                BsonConstants.BSON_KEY_PROVIDER_METADATA_PV_NAMES,
                                                "$" + BsonConstants.BSON_KEY_PV_NAME),
                                        Accumulators.first(
                                                // save the first time of the first bucket document for this provider
                                                BsonConstants.BSON_KEY_PROVIDER_METADATA_FIRST_BUCKET_TIMESTAMP,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_FIRST_TIME),
                                        Accumulators.last(
                                                // save the first time of the last bucket document for this provider
                                                BsonConstants.BSON_KEY_PROVIDER_METADATA_LAST_BUCKET_TIMESTAMP,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_FIRST_TIME),
                                        Accumulators.sum(
                                                // count number of bucket documents in group for this provider
                                                BsonConstants.BSON_KEY_PROVIDER_METADATA_NUM_BUCKETS,
                                                1)
                                ),
                                Aggregates.sort(metadataSort) // sort metadata documents so result is sorted
                        ));

        // aggregateIterable.forEach(bucketDocument -> {System.out.println(bucketDocument.toString());});

        return aggregateIterable.cursor();
    }

}
