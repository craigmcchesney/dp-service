package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.client.model.Indexes;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDataFrameDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.column.ColumnDocumentBase;
import com.ospreydcs.dp.service.common.bson.column.DataColumnDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.bson.bucket.*;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.bson.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public abstract class MongoClientBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();
    private static String mongoDatabaseName = null;

    // constants
    public static final String ADMIN_DATABASE_NAME = "admin";
    public static final String MONGO_DATABASE_NAME = "dp";
    public static final String COLLECTION_NAME_PROVIDERS = "providers";
    public static final String COLLECTION_NAME_BUCKETS = "buckets";
    public static final String COLLECTION_NAME_REQUEST_STATUS = "requestStatus";
    public static final String COLLECTION_NAME_DATA_SETS = "dataSets";
    public static final String COLLECTION_NAME_ANNOTATIONS = "annotations";
    public static final String COLLECTION_NAME_CALCULATIONS = "calculations";

    // configuration
    public static final int DEFAULT_NUM_WORKERS = 7;
    public static final String CFG_KEY_DB_URI = "MongoClient.uri";
    public static final String DEFAULT_DB_URI = "mongodb://admin:admin@localhost:27017/";


    // abstract methods
    protected abstract boolean initMongoClient(String connectString);
    protected abstract boolean initMongoDatabase(String databaseName, CodecRegistry codecRegistry);
    protected abstract boolean initMongoCollectionProviders(String collectionName);
    protected abstract boolean createMongoIndexProviders(Bson fieldNamesBson);
    protected abstract boolean initMongoCollectionBuckets(String collectionName);
    protected abstract boolean createMongoIndexBuckets(Bson fieldNamesBson);
    protected abstract boolean initMongoCollectionRequestStatus(String collectionName);
    protected abstract boolean createMongoIndexRequestStatus(Bson fieldNamesBson);
    protected abstract boolean initMongoCollectionDataSets(String collectionName);
    protected abstract boolean createMongoIndexDataSets(Bson fieldNamesBson);
    protected abstract boolean initMongoCollectionAnnotations(String collectionName);
    protected abstract boolean createMongoIndexAnnotations(Bson fieldNamesBson);
    protected abstract boolean initMongoCollectionCalculations(String collectionName);
    protected abstract boolean createMongoIndexCalculations(Bson fieldNamesBson);

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    protected CodecRegistry getPojoCodecRegistry() {

        // set up mongo codec registry for handling pojos automatically
        // create mongo codecs for model classes

//        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().register(TsDataBucket.class, DatumModel.class).build();

        // Registration by packageName led to an exception in the query service when iterating result cursor,
        // see details below about registrering classes explicitly.
//        String packageName = BucketDocument.class.getPackageName();
//        LOGGER.trace("CodecProvider registering packageName: " + packageName);
//        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().register(packageName).build();

        // Was registering POJO classes with CodecProvider by packageName as shown above, but this doesn't work
        // when using find with a cursor.  I got an exception "Decoding errored with: A class could not be found for the discriminator: DOUBLE"
        // "A custom Codec or PojoCodec may need to be explicitly configured and registered to handle this type."
        // Indeed, registering the classes explicitly solved that problem but sort of a bummer because any new ones must
        // be explicitly registered here.
        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().register(
                ProviderDocument.class,
                BucketDocument.class,
                EventMetadataDocument.class,
                RequestStatusDocument.class,
                AnnotationDocument.class,
                DataSetDocument.class,
                DataBlockDocument.class,
                PvMetadataQueryResultDocument.class,
                ProviderMetadataQueryResultDocument.class,
                CalculationsDocument.class,
                CalculationsDataFrameDocument.class,
                TimestampDocument.class,
                DataTimestampsDocument.class,
                ColumnDocumentBase.class,
                DataColumnDocument.class
        ).build();

        //        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();

        CodecRegistry pojoCodecRegistry =
                fromRegistries(getDefaultCodecRegistry(), fromProviders(pojoCodecProvider));
        return pojoCodecRegistry;
    }

    private boolean createMongoIndexesProviders() {

        // regular index on name field
        createMongoIndexProviders(Indexes.ascending(BsonConstants.BSON_KEY_PROVIDER_NAME));

        // text index on name and description fields
        createMongoIndexProviders(
                Indexes.compoundIndex(
                        Indexes.text(BsonConstants.BSON_KEY_DATA_SET_NAME),
                        Indexes.text(BsonConstants.BSON_KEY_DATA_SET_DESCRIPTION)));

        // create index by tags
        createMongoIndexProviders(Indexes.ascending(BsonConstants.BSON_KEY_TAGS));

        // create index by attributes
        createMongoIndexProviders(
                Indexes.ascending(BsonConstants.BSON_KEY_ATTRIBUTES + ".$**"));

        return true;
    }

    private boolean createMongoIndexesBuckets() {

        // regular index by name
        createMongoIndexBuckets(Indexes.ascending(BsonConstants.BSON_KEY_PV_NAME));

        // compound index by name and time fields (used in bucket data queries)
        createMongoIndexBuckets(Indexes.ascending(
                BsonConstants.BSON_KEY_PV_NAME,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS,
                BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS,
                BsonConstants.BSON_KEY_BUCKET_LAST_TIME_NANOS));

        // regular index on providerId field
        createMongoIndexBuckets(Indexes.ascending(BsonConstants.BSON_KEY_BUCKET_PROVIDER_ID));

        return true;
    }

    private boolean createMongoIndexesRequestStatus() {
        createMongoIndexRequestStatus(Indexes.ascending(
                BsonConstants.BSON_KEY_REQ_STATUS_PROVIDER_ID,
                BsonConstants.BSON_KEY_REQ_STATUS_REQUEST_ID));
        createMongoIndexRequestStatus(Indexes.ascending(
                BsonConstants.BSON_KEY_REQ_STATUS_PROVIDER_ID,
                BsonConstants.BSON_KEY_REQ_STATUS_STATUS,
                BsonConstants.BSON_KEY_CREATED_AT));
        createMongoIndexRequestStatus(Indexes.ascending(
                BsonConstants.BSON_KEY_REQ_STATUS_STATUS,
                BsonConstants.BSON_KEY_CREATED_AT));
        return true;
    }

    private boolean createMongoIndexesDataSets() {

        // create regular index by ownerId
        createMongoIndexDataSets(Indexes.ascending(BsonConstants.BSON_KEY_DATA_SET_OWNER_ID));

        // create compound index with text index on name / description and regular index on owner id
        // NOTE - reordering so that owner id comes before the text index can lead to an error message like this:
        // "if text index is compound, are equality predicates given for all prefix fields?"
        // discussed here: https://stackoverflow.com/questions/35260539/combine-full-text-with-other-index
        createMongoIndexDataSets(
                Indexes.compoundIndex(
                        Indexes.compoundIndex(
                                Indexes.text(BsonConstants.BSON_KEY_DATA_SET_NAME),
                                Indexes.text(BsonConstants.BSON_KEY_DATA_SET_DESCRIPTION)),
                        Indexes.ascending(BsonConstants.BSON_KEY_DATA_SET_OWNER_ID)));

        // create index on data block pvNames
        createMongoIndexDataSets(Indexes.ascending(BsonConstants.BSON_KEY_DATA_SET_BLOCK_PV_NAMES));

        return true;
    }

    private boolean createMongoIndexesAnnotations() {

        // create index by ownerId
        createMongoIndexAnnotations(Indexes.ascending(BsonConstants.BSON_KEY_ANNOTATION_OWNER_ID));

        // create index by id of associated datasets
        createMongoIndexAnnotations(Indexes.ascending(BsonConstants.BSON_KEY_ANNOTATION_DATASET_IDS));

        // create index by id of associated annotations
        createMongoIndexAnnotations(Indexes.ascending(BsonConstants.BSON_KEY_ANNOTATION_ANNOTATION_IDS));

        // create index by tags
        createMongoIndexAnnotations(Indexes.ascending(BsonConstants.BSON_KEY_TAGS));

        // create index by attributes
        createMongoIndexAnnotations(
                Indexes.ascending(BsonConstants.BSON_KEY_ATTRIBUTES + ".$**"));

        // create compound index on type/comment to optimize searching comment annotation text
        // NOTE - reordering so that owner id comes before the text index can lead to an error message like this:
        // "if text index is compound, are equality predicates given for all prefix fields?"
        // discussed here: https://stackoverflow.com/questions/35260539/combine-full-text-with-other-index
        createMongoIndexAnnotations(
                Indexes.compoundIndex(
                        Indexes.compoundIndex(
                            Indexes.text(BsonConstants.BSON_KEY_ANNOTATION_NAME),
                            Indexes.text(BsonConstants.BSON_KEY_ANNOTATION_COMMENT),
                            Indexes.text(BsonConstants.BSON_KEY_EVENT_DESCRIPTION)),
                        Indexes.ascending(BsonConstants.BSON_KEY_ANNOTATION_OWNER_ID)));

        return true;
    }

    private boolean createMongoIndexesCalculations() {
        return true;
    }

    public static String getMongoConnectString() {
        // Allow a full connection string override to support replica sets, TLS, options, etc.
        String uriOverride = configMgr().getConfigString(CFG_KEY_DB_URI, DEFAULT_DB_URI);
        return uriOverride;
    }

    protected static String getMongoDatabaseName() {
        if (mongoDatabaseName == null) {
            return MONGO_DATABASE_NAME;
        } else {
            return mongoDatabaseName;
        }
    }

    protected static void setMongoDatabaseName(String databaseName) {
        if (databaseName.isBlank()) {
            logger.error("setDatabaseName specified database name is empty");
        } else {
            mongoDatabaseName = databaseName;
        }
    }

    protected String getCollectionNameProviders() {
        return COLLECTION_NAME_PROVIDERS;
    }

    protected String getCollectionNameBuckets() {
        return COLLECTION_NAME_BUCKETS;
    }

    protected String getCollectionNameRequestStatus() {
        return COLLECTION_NAME_REQUEST_STATUS;
    }

    protected String getCollectionNameDataSets() {
        return COLLECTION_NAME_DATA_SETS;
    }

    protected String getCollectionNameAnnotations() {
        return COLLECTION_NAME_ANNOTATIONS;
    }

    protected String getCollectionNameCalculations() {
        return COLLECTION_NAME_CALCULATIONS;
    }

    public boolean init() {

        logger.trace("init");

        String connectString = getMongoConnectString();
        String databaseName = getMongoDatabaseName();
        String collectionNameProviders = getCollectionNameProviders();
        String collectionNameBuckets = getCollectionNameBuckets();
        String collectionNameRequestStatus = getCollectionNameRequestStatus();
        String collectionNameDataSets = getCollectionNameDataSets();
        String collectionNameAnnotations = getCollectionNameAnnotations();
        String collectionNameCalculations = getCollectionNameCalculations();
        logger.info("mongo client init connectString: {} databaseName: {}", connectString, databaseName);
        logger.info(
                "mongo client init collection names "
                        + "annotations: {} buckets: {} calculations: {} datasets: {} providers: {} requestStatus: {}",
                collectionNameAnnotations,
                collectionNameBuckets,
                collectionNameCalculations,
                collectionNameDataSets,
                collectionNameProviders,
                collectionNameRequestStatus);

        // connect mongo client
        initMongoClient(connectString);

        // connect to database
        initMongoDatabase(databaseName, getPojoCodecRegistry());

        // initialize providers collection
        initMongoCollectionProviders(collectionNameProviders);
        createMongoIndexesProviders();

        // initialize buckets collection
        initMongoCollectionBuckets(collectionNameBuckets);
        createMongoIndexesBuckets();

        // initialize request status collection
        initMongoCollectionRequestStatus(collectionNameRequestStatus);
        createMongoIndexesRequestStatus();

        // initialize datasets collection
        initMongoCollectionDataSets(collectionNameDataSets);
        createMongoIndexesDataSets();

        // initialize annotations collection
        initMongoCollectionAnnotations(collectionNameAnnotations);
        createMongoIndexesAnnotations();

        // initialize calculations collection
        initMongoCollectionCalculations(collectionNameCalculations);
        createMongoIndexesCalculations();

        return true;
    }

    public boolean fini() {
        logger.trace("fini");
        return true;
    }
}
