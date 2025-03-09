package com.ospreydcs.dp.service.annotation.handler.mongo.export;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;

import java.io.File;
import java.util.Date;
import java.util.Objects;

/*
 * Export file directory structure (using hdf5 groups):
 *
 * /root/dataset : Contains details about the dataset exported to this file, with list of pvs and begin/end times
 * for each data block (TODO).
 *
 * /root/pvs : Contains index structure by pv name and bucket first time (seconds and nanos), where each leaf directory
 * contains the bucket document fields for the bucket whose first time matches the directory path's seconds and nanos.
 * E.g., "/pvs/S01-GCC01/times/001727467265/000353987280" contains fields for the S01-GCC01 bucket document whose first
 * time seconds is 001727467265 and nanos 000353987280.
 *
 * /root/times : Contains index structure by 1) the first time seconds and nanos fields of the dataset's buckets and
 * 2) the pv names with buckets whose first time matches the path's seconds and nanos. The entry for each pv name is a
 * soft link to the directory containing the bucket document fields for the bucket for that pv whose first time matches
 * the path name's seconds and nanos.  E.g., "/root/times/001727467265/000353987280/pvs/S01-GCC01" is linked to
 * "/pvs/S01-GCC01/times/001727467265/000353987280", so the bucket fields described above are also available
 * by navigating the soft link.
 */

public class DatasetExportHdf5File implements BucketedDataExportFileInterface {

    // constants
    public final static String GROUP_DATASET = "dataset";
    public final static String GROUP_PVS = "pvs";
    public final static String GROUP_TIMES = "times";
    public final static String GROUP_DATA_BLOCKS = "datablocks";
    public final static String DATASET_BLOCK_PV_NAME_LIST = "pvNameList";
    public final static String DATASET_BLOCK_BEGIN_SECONDS = "beginSeconds";
    public final static String DATASET_BLOCK_BEGIN_NANOS = "beginNanos";
    public final static String DATASET_BLOCK_END_SECONDS = "endSeconds";
    public final static String DATASET_BLOCK_END_NANOS = "endNanos";
    public final static String DATASET_FIRST_SECONDS = "firstSeconds";
    public final static String DATASET_FIRST_NANOS = "firstNanos";
    public final static String DATASET_FIRST_TIME = "firstTime";
    public final static String DATASET_LAST_SECONDS = "lastSeconds";
    public final static String DATASET_LAST_NANOS = "lastNanos";
    public final static String DATASET_LAST_TIME = "lastTime";
    public final static String DATASET_SAMPLE_COUNT = "sampleCount";
    public final static String DATASET_SAMPLE_PERIOD = "samplePeriod";
    public final static String DATASET_DATA_COLUMN_BYTES = "dataColumnBytes";
    public final static String DATASET_DATA_TIMESTAMPS_BYTES = "dataTimestampsBytes";
    public final static String DATASET_ATTRIBUTE_MAP_KEYS = "attributeMapKeys";
    public final static String DATASET_ATTRIBUTE_MAP_VALUES = "attributeMapValues";
    public final static String DATASET_EVENT_METADATA_DESCRIPTION = "eventMetadataDescription";
    public final static String DATASET_EVENT_METADATA_START_SECONDS = "eventMetadataStartSeconds";
    public final static String DATASET_EVENT_METADATA_START_NANOS = "eventMetadataStartNanos";
    public final static String DATASET_EVENT_METADATA_STOP_SECONDS = "eventMetadataStopSeconds";
    public final static String DATASET_EVENT_METADATA_STOP_NANOS = "eventMetadataStopNanos";
    public final static String DATASET_PROVIDER_ID = "providerId";
    public final static String PATH_SEPARATOR = "/";

    // instance variables
    private final IHDF5Writer writer;

    public DatasetExportHdf5File(DataSetDocument dataSet, String filePathString) throws DpException {
        // create hdf5 file with specified path
        File hdf5File = new File(filePathString);
//        if (hdf5File.canWrite()) {
//            throw new IOException("unable to write to hdf5 file: " + filePathString);
//        }
        writer = HDF5Factory.configure(hdf5File).overwrite().writer();
        this.initialize(dataSet);
    }

    private void initialize(DataSetDocument dataSet) {
        this.createGroups();
        this.writeDataSetData(dataSet);
    }

    public void createGroups() {
        // create top-level groups for file organization
        writer.object().createGroup(GROUP_DATASET);
        writer.object().createGroup(GROUP_PVS);
        writer.object().createGroup(GROUP_TIMES);
    }

    public void writeDataSetData(DataSetDocument dataSet) {

        // create dataset base paths
        final String dataBlocksGroup = PATH_SEPARATOR
                + GROUP_DATASET
                + PATH_SEPARATOR
                + GROUP_DATA_BLOCKS;
        writer.object().createGroup(dataBlocksGroup);

        int dataBlockIndex = 0;
        for (DataBlockDocument dataBlock : dataSet.getDataBlocks()) {
            final String dataBlockIndexGroup = dataBlocksGroup
                    + PATH_SEPARATOR
                    + dataBlockIndex;
            writer.object().createGroup(dataBlockIndexGroup);
            final String dataBlockPathBase = dataBlockIndexGroup + PATH_SEPARATOR;
            final String pvNameListPath = dataBlockPathBase + DATASET_BLOCK_PV_NAME_LIST;
            writer.writeStringArray(pvNameListPath, dataBlock.getPvNames().toArray(new String[0]));
            final String beginTimeSecondsPath = dataBlockPathBase + DATASET_BLOCK_BEGIN_SECONDS;
            writer.writeLong(beginTimeSecondsPath, dataBlock.getBeginTime().getSeconds());
            final String beginTimeNanosPath = dataBlockPathBase + DATASET_BLOCK_BEGIN_NANOS;
            writer.writeLong(beginTimeNanosPath, dataBlock.getBeginTime().getNanos());
            final String endTimeSecondsPath = dataBlockPathBase + DATASET_BLOCK_END_SECONDS;
            writer.writeLong(endTimeSecondsPath, dataBlock.getEndTime().getSeconds());
            final String endTimeNanosPath = dataBlockPathBase + DATASET_BLOCK_END_NANOS;
            writer.writeLong(endTimeNanosPath, dataBlock.getEndTime().getNanos());
            dataBlockIndex = dataBlockIndex + 1;
        }
    }

    public void writeBucketData(BucketDocument bucketDocument) {

        // create groups for indexing by pv and time
        Objects.requireNonNull(bucketDocument.getPvName());
        final String pvNameGroup = GROUP_PVS + PATH_SEPARATOR + bucketDocument.getPvName();
        if (! writer.object().isGroup(pvNameGroup)) {
            writer.object().createGroup(pvNameGroup);
        }
        final String pvTimesGroup = pvNameGroup + PATH_SEPARATOR + GROUP_TIMES;
        if (! writer.object().isGroup(pvTimesGroup)) {
            writer.object().createGroup(pvTimesGroup);
        }
        long firstSeconds = bucketDocument.getDataTimestamps().getFirstTime().getSeconds();
        final String firstSecondsString = String.format("%012d", firstSeconds);
        final String pvTimesSecondsGroup = pvTimesGroup + PATH_SEPARATOR + firstSecondsString;
        if (! writer.object().isGroup(pvTimesSecondsGroup)) {
            writer.object().createGroup(pvTimesSecondsGroup);
        }
        long firstNanos = bucketDocument.getDataTimestamps().getFirstTime().getNanos();
        final String firstNanosString = String.format("%012d", firstNanos);
        final String pvTimesSecondsNanosGroup = pvTimesSecondsGroup + PATH_SEPARATOR + firstNanosString;
        if (! writer.object().isGroup(pvTimesSecondsNanosGroup)) {
            writer.object().createGroup(pvTimesSecondsNanosGroup);
        }

        // write fields from bucket document (including column data values) document under pv index

        // first seconds/nanos/time
        Date firstTime = bucketDocument.getDataTimestamps().getFirstTime().getDateTime();
        Objects.requireNonNull(firstTime);
        final String firstSecondsPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_FIRST_SECONDS;
        writer.writeLong(firstSecondsPath, firstSeconds);
        final String firstNanosPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_FIRST_NANOS;
        writer.writeLong(firstNanosPath, firstNanos);
        final String firstTimePath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_FIRST_TIME;
        writer.time().write(firstTimePath, firstTime);

        // last seconds/nanos/time
        long lastSeconds = bucketDocument.getDataTimestamps().getLastTime().getSeconds();
        long lastNanos = bucketDocument.getDataTimestamps().getLastTime().getNanos();
        Date lastTime = bucketDocument.getDataTimestamps().getLastTime().getDateTime();
        Objects.requireNonNull(lastSeconds);
        Objects.requireNonNull(lastNanos);
        Objects.requireNonNull(lastTime);
        final String lastSecondsPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_LAST_SECONDS;
        writer.writeLong(lastSecondsPath, lastSeconds);
        final String lastNanosPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_LAST_NANOS;
        writer.writeLong(lastNanosPath, lastNanos);
        final String lastTimePath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_LAST_TIME;
        writer.time().write(lastTimePath, lastTime);

        // sample period and count
        Objects.requireNonNull(bucketDocument.getDataTimestamps().getSampleCount());
        Objects.requireNonNull(bucketDocument.getDataTimestamps().getSamplePeriod());
        final String sampleCountPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_SAMPLE_COUNT;
        writer.writeInt(sampleCountPath, bucketDocument.getDataTimestamps().getSampleCount());
        final String samplePeriodPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_SAMPLE_PERIOD;
        writer.writeLong(samplePeriodPath, bucketDocument.getDataTimestamps().getSamplePeriod());

        // DataColumn serialized bytes
        Objects.requireNonNull(bucketDocument.getDataColumn());
        final byte[] dataColumnBytes = bucketDocument.getDataColumn().getBytes();
        Objects.requireNonNull(dataColumnBytes);
        final String columnDataPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_DATA_COLUMN_BYTES;
        writer.writeByteArray(columnDataPath, dataColumnBytes);

        // dataTimestampsBytes
        Objects.requireNonNull(bucketDocument.getDataTimestamps().getBytes());
        final String dataTimestampsPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_DATA_TIMESTAMPS_BYTES;
        writer.writeByteArray(dataTimestampsPath, bucketDocument.getDataTimestamps().getBytes());

        // attributeMap - write keys to one array and values to another
        Objects.requireNonNull(bucketDocument.getAttributes());
        final String attributeMapKeysPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_ATTRIBUTE_MAP_KEYS;
        writer.writeStringArray(attributeMapKeysPath, bucketDocument.getAttributes().keySet().toArray(new String[0]));
        final String attributeMapValuesPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_ATTRIBUTE_MAP_VALUES;
        writer.writeStringArray(attributeMapValuesPath, bucketDocument.getAttributes().values().toArray(new String[0]));

        // eventMetadata - description, start/stop times
        Objects.requireNonNull(bucketDocument.getEvent());
        final String eventMetadataDescriptionPath = 
                pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_EVENT_METADATA_DESCRIPTION;
        writer.writeString(eventMetadataDescriptionPath, bucketDocument.getEvent().getDescription());

        if (bucketDocument.getEvent().getStartTime() != null) {
            final String eventMetadataStartSecondsPath =
                    pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_EVENT_METADATA_START_SECONDS;
            writer.writeLong(
                    eventMetadataStartSecondsPath,
                    bucketDocument.getEvent().getStartTime().getSeconds());
            final String eventMetadataStartNanosPath =
                    pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_EVENT_METADATA_START_NANOS;
            writer.writeLong(
                    eventMetadataStartNanosPath,
                    bucketDocument.getEvent().getStartTime().getNanos());
        }

        if (bucketDocument.getEvent().getStopTime() != null) {
            final String eventMetadataStopSecondsPath =
                    pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_EVENT_METADATA_STOP_SECONDS;
            writer.writeLong(
                    eventMetadataStopSecondsPath,
                    bucketDocument.getEvent().getStopTime().getSeconds());
            final String eventMetadataStopNanosPath =
                    pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_EVENT_METADATA_STOP_NANOS;
            writer.writeLong(
                    eventMetadataStopNanosPath,
                    bucketDocument.getEvent().getStopTime().getNanos());
        }

        // providerId
        Objects.requireNonNull(bucketDocument.getProviderId());
        final String providerIdPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_PROVIDER_ID;
        writer.writeString(providerIdPath, bucketDocument.getProviderId());
        
        // create groups for indexing by time and pv
        final String timesSecondsGroup = GROUP_TIMES + PATH_SEPARATOR + firstSecondsString;
        if (! writer.object().isGroup(timesSecondsGroup)) {
            writer.object().createGroup(timesSecondsGroup);
        }
        final String timesSecondsNanosGroup = timesSecondsGroup + PATH_SEPARATOR + firstNanosString;
        if (! writer.object().isGroup(timesSecondsNanosGroup)) {
            writer.object().createGroup(timesSecondsNanosGroup);
        }
        final String timesSecondsNanosPvsGroup = timesSecondsNanosGroup + PATH_SEPARATOR + GROUP_PVS;
        if (! writer.object().isGroup(timesSecondsNanosPvsGroup)) {
            writer.object().createGroup(timesSecondsNanosPvsGroup);
        }

        // create soft link to bucket document under pvs path from times path
        final String timesSecondsNanosPvsPvPath =
                timesSecondsNanosPvsGroup + PATH_SEPARATOR + bucketDocument.getPvName();
        if (! writer.object().exists(timesSecondsNanosPvsPvPath)) {
            writer.object().createSoftLink(PATH_SEPARATOR + pvTimesSecondsNanosGroup, timesSecondsNanosPvsPvPath);
        }
    }

    public void close() {
        writer.close();
    }
}