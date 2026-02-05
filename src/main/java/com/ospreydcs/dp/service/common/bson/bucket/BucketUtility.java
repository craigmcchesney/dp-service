package com.ospreydcs.dp.service.common.bson.bucket;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.common.bson.column.DataColumnDocument;
import com.ospreydcs.dp.service.common.bson.DataTimestampsDocument;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.round;

public class BucketUtility {
    public static List<BucketDocument> createBucketDocuments(
            long firstSeconds,
            int numSamplesPerSecond,
            int numSecondsPerBucket,
            String columnNameBase,
            int numColumns,
            int numBucketsPerColumn
    ) {
        final List<BucketDocument> bucketList = new ArrayList<>();

        final double sampleRate = 1.0 / numSamplesPerSecond;
        final int sampleCount = numSamplesPerSecond * numSecondsPerBucket;
        final long samplePeriod = round(sampleRate * 1_000_000_000L);

        long firstTimeSeconds = firstSeconds;
        long firstTimeNanos = 0L;
        long lastTimeSeconds = firstTimeSeconds + numSecondsPerBucket - 1;

        for (int batchIndex = 0 ; batchIndex < numBucketsPerColumn ; batchIndex++) {

            // create a bucket document for each column in the batch
            for (int columnIndex = 1 ; columnIndex <= numColumns ; columnIndex++) {

                final String pvName = columnNameBase + columnIndex;
                final String documentId = pvName + "-" + firstTimeSeconds + "-" + firstTimeNanos;
                final BucketDocument bucket = new BucketDocument();
                bucket.setId(documentId);
                bucket.setPvName(pvName);

                // create DataColumn with specified number of values and add it to BucketDocument
                final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
                dataColumnBuilder.setName(pvName);
                for (int samplesIndex = 0 ; samplesIndex < sampleCount ; samplesIndex++) {
                    final double doubleValue = samplesIndex;
                    final DataValue dataValue = DataValue.newBuilder().setDoubleValue(doubleValue).build();
                    dataColumnBuilder.addDataValues(dataValue);
                }
                final DataColumnDocument dataColumnDocument = DataColumnDocument.fromDataColumn(dataColumnBuilder.build());
                bucket.setDataColumn(dataColumnDocument);

                // set DataTimestamps in bucket
                final Timestamp startTime = TimestampUtility.timestampFromSeconds(firstTimeSeconds, firstTimeNanos);
                final SamplingClock samplingClock = SamplingClock.newBuilder()
                        .setStartTime(startTime)
                        .setPeriodNanos(samplePeriod)
                        .setCount(sampleCount)
                        .build();
                final DataTimestamps dataTimestamps = DataTimestamps.newBuilder().setSamplingClock(samplingClock).build();
                final DataTimestampsDocument dataTimestampsDocument =
                        DataTimestampsDocument.fromDataTimestamps(dataTimestamps);
                bucket.setDataTimestamps(dataTimestampsDocument);

                bucketList.add(bucket);
            }

            // update first/last time for next batch
            firstTimeSeconds = lastTimeSeconds + 1;
            firstTimeNanos = 0L;
            lastTimeSeconds = firstTimeSeconds + numSecondsPerBucket - 1;
        }

        return bucketList;
    }

}
