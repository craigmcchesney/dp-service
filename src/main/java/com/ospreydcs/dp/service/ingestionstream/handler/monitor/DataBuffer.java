package com.ospreydcs.dp.service.ingestionstream.handler.monitor;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataBuffer {

    private static final Logger logger = LogManager.getLogger();

    private final String pvName;
    private final DataBufferConfig config;
    private final List<BufferedDataItem> bufferedItems = new ArrayList<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();
    
    private long currentBufferSizeBytes = 0;
    private Instant lastFlushTime = Instant.now();

    public static class DataBufferConfig {
        private final long flushIntervalMs;
        private final long maxBufferSizeBytes;
        private final int maxBufferItems;
        private final long maxItemAgeNanos;

        public DataBufferConfig(long flushIntervalMs, long maxBufferSizeBytes, int maxBufferItems, long maxItemAgeNanos) {
            this.flushIntervalMs = flushIntervalMs;
            this.maxBufferSizeBytes = maxBufferSizeBytes;
            this.maxBufferItems = maxBufferItems;
            this.maxItemAgeNanos = maxItemAgeNanos;
        }

        public long getFlushIntervalMs() { return flushIntervalMs; }
        public long getMaxBufferSizeBytes() { return maxBufferSizeBytes; }
        public int getMaxBufferItems() { return maxBufferItems; }
        public long getMaxItemAgeNanos() { return maxItemAgeNanos; }

        // Convenience methods for time conversion
        public static long millisecondsToNanos(long milliseconds) {
            return milliseconds * 1_000_000L;
        }

        public static long secondsToNanos(long seconds) {
            return seconds * 1_000_000_000L;
        }
    }

    private static class BufferedDataItem {
        private DataColumn dataColumn = null;
        private SerializedDataColumn serializedDataColumn = null;
        private final DataTimestamps dataTimestamps;
        private final Instant timestamp;
        private final long estimatedSizeBytes;

        private BufferedDataItem(
                DataTimestamps dataTimestamps,
                long estimatedSizeBytes
        ) {
            this.dataTimestamps = dataTimestamps;
            this.estimatedSizeBytes = estimatedSizeBytes;
            this.timestamp = Instant.now();
        }

        public BufferedDataItem(
                DataColumn dataColumn,
                DataTimestamps dataTimestamps,
                long estimatedSizeBytes
        ) {
            this(dataTimestamps, estimatedSizeBytes);
            this.dataColumn = dataColumn;
        }

        public BufferedDataItem(
                SerializedDataColumn serializedDataColumn,
                DataTimestamps dataTimestamps,
                long estimatedSizeBytes
        ) {
            this(dataTimestamps, estimatedSizeBytes);
            this.serializedDataColumn = serializedDataColumn;
        }

        public DataColumn getDataColumn() { return dataColumn; }
        public DataTimestamps getDataTimestamps() { return dataTimestamps; }
        public Instant getTimestamp() { return timestamp; }
        public long getEstimatedSizeBytes() { return estimatedSizeBytes; }
        public SerializedDataColumn getSerializedDataColumn() { return serializedDataColumn; }
    }

    public static class BufferedData {
        private final DataColumn dataColumn;
        private final SerializedDataColumn serializedDataColumn;
        private final DataTimestamps dataTimestamps;
        private final long estimatedSize;
        private final Instant firstInstant;
        private final Instant lastInstant;

        public BufferedData(BufferedDataItem bufferedDataItem) {
            this.dataColumn = bufferedDataItem.getDataColumn();
            this.serializedDataColumn = bufferedDataItem.getSerializedDataColumn();
            this.dataTimestamps = bufferedDataItem.getDataTimestamps();
            this.estimatedSize = bufferedDataItem.getEstimatedSizeBytes();

            // set begin / end times from dataTimestamps
            final DataTimestampsUtility.DataTimestampsModel dataTimestampsModel =
                    new DataTimestampsUtility.DataTimestampsModel(dataTimestamps);
            firstInstant = TimestampUtility.instantFromTimestamp(dataTimestampsModel.getFirstTimestamp());
            lastInstant = TimestampUtility.instantFromTimestamp(dataTimestampsModel.getLastTimestamp());
        }

        public DataColumn getDataColumn() { return dataColumn; }
        public SerializedDataColumn getSerializedDataColumn() { return serializedDataColumn; }
        public DataTimestamps getDataTimestamps() { return dataTimestamps; }
        public long getEstimatedSize() { return estimatedSize; }
        public Instant getFirstInstant() { return firstInstant; }
        public Instant getLastInstant() { return lastInstant; }
    }

    public DataBuffer(String pvName, DataBufferConfig config) {
        this.pvName = pvName;
        this.config = config;
    }

    public void addData(DataColumn dataColumn, DataTimestamps dataTimestamps) {
        writeLock.lock();
        try {
            long estimatedSize = estimateDataSize(dataColumn);
            BufferedDataItem item = new BufferedDataItem(dataColumn, dataTimestamps, estimatedSize);
            
            bufferedItems.add(item);
            currentBufferSizeBytes += estimatedSize;
            
            logger.debug("Added DataColumn to buffer for PV: {}, buffer size: {} bytes, {} items",
                        pvName, currentBufferSizeBytes, bufferedItems.size());
        } finally {
            writeLock.unlock();
        }
    }

    public void addSerializedData(SerializedDataColumn serializedDataColumn, DataTimestamps dataTimestamps) {
        writeLock.lock();
        try {
            long estimatedSize = estimateSerializedDataSize(serializedDataColumn);
            BufferedDataItem item = new BufferedDataItem(serializedDataColumn, dataTimestamps, estimatedSize);

            bufferedItems.add(item);
            currentBufferSizeBytes += estimatedSize;

            logger.debug("Added SerializedDataColumn to buffer for PV: {}, buffer size: {} bytes, {} items",
                    pvName, currentBufferSizeBytes, bufferedItems.size());
        } finally {
            writeLock.unlock();
        }
    }

    public boolean shouldFlush() {
        readLock.lock();
        try {
            if (bufferedItems.isEmpty()) {
                return false;
            }

            Instant now = Instant.now();
            long timeSinceLastFlush = now.toEpochMilli() - lastFlushTime.toEpochMilli();
            
            // Check if any items have exceeded max age
            boolean hasExpiredItems = bufferedItems.stream()
                .anyMatch(item -> {
                    Duration itemAge = Duration.between(item.getTimestamp(), now);
                    return itemAge.toNanos() >= config.getMaxItemAgeNanos();
                });
            
            return timeSinceLastFlush >= config.getFlushIntervalMs() ||
                   currentBufferSizeBytes >= config.getMaxBufferSizeBytes() ||
                   bufferedItems.size() >= config.getMaxBufferItems() ||
                   hasExpiredItems;
        } finally {
            readLock.unlock();
        }
    }

    public List<BufferedData> flush() {
        writeLock.lock();
        try {
            if (bufferedItems.isEmpty()) {
                return new ArrayList<>();
            }

            Instant now = Instant.now();
            List<BufferedData> results = new ArrayList<>();
            List<BufferedDataItem> itemsToRemove = new ArrayList<>();
            
            // Only flush items that have reached the configured age
            for (BufferedDataItem item : bufferedItems) {
                Duration itemAge = Duration.between(item.getTimestamp(), now);
                if (itemAge.toNanos() >= config.getMaxItemAgeNanos()) {
                    results.add(new BufferedData(item));
                    itemsToRemove.add(item);
                }
            }

            // Remove flushed items and update buffer size
            for (BufferedDataItem item : itemsToRemove) {
                bufferedItems.remove(item);
                currentBufferSizeBytes -= item.getEstimatedSizeBytes();
            }

            if (!results.isEmpty()) {
                logger.debug("Flushing {} aged items from buffer for PV: {}, {} items remaining, {} bytes remaining", 
                            results.size(), pvName, bufferedItems.size(), currentBufferSizeBytes);
            }

            lastFlushTime = now;
            return results;
        } finally {
            writeLock.unlock();
        }
    }

    public int getBufferedItemCount() {
        readLock.lock();
        try {
            return bufferedItems.size();
        } finally {
            readLock.unlock();
        }
    }

    public long getCurrentBufferSizeBytes() {
        readLock.lock();
        try {
            return currentBufferSizeBytes;
        } finally {
            readLock.unlock();
        }
    }

    private long estimateDataSize(DataColumn dataColumn) {
        AtomicLong size = new AtomicLong(100); // Base overhead for timestamps and structure
        
        size.addAndGet(dataColumn.getName().length() * 2); // String overhead
        size.addAndGet(dataColumn.getDataValuesList().size() * 50); // Base per-value overhead
        
        dataColumn.getDataValuesList().forEach(dataValue -> {
            switch (dataValue.getValueCase()) {
                case STRINGVALUE:
                    size.addAndGet(dataValue.getStringValue().length() * 2);
                    break;
                case BYTEARRAYVALUE:
                    size.addAndGet(dataValue.getByteArrayValue().size());
                    break;
                case ARRAYVALUE:
                    size.addAndGet(dataValue.getArrayValue().getDataValuesCount() * 32);
                    break;
                case STRUCTUREVALUE:
                    size.addAndGet(dataValue.getStructureValue().getFieldsCount() * 64);
                    break;
                case IMAGEVALUE:
                    size.addAndGet(dataValue.getImageValue().getImage().size());
                    break;
                default:
                    size.addAndGet(8); // Primitive types
                    break;
            }
        });
        
        return size.get();
    }

    private long estimateSerializedDataSize(SerializedDataColumn serializedDataColumn) {
        AtomicLong size = new AtomicLong(100); // Base overhead for timestamps and structure
        size.addAndGet(serializedDataColumn.getName().length() * 2); // String overhead
        size.addAndGet(50); // Base overhead for data column bytes.
        size.addAndGet(serializedDataColumn.getSerializedSize());
        return size.get();
    }

    public List<BufferedData> forceFlushAll() {
        writeLock.lock();
        try {
            if (bufferedItems.isEmpty()) {
                return new ArrayList<>();
            }

            List<BufferedData> results = new ArrayList<>();
            for (BufferedDataItem item : bufferedItems) {
                results.add(new BufferedData(item));
            }

            logger.debug("Force flushing all {} items from buffer for PV: {}, {} bytes", 
                        bufferedItems.size(), pvName, currentBufferSizeBytes);

            bufferedItems.clear();
            currentBufferSizeBytes = 0;
            lastFlushTime = Instant.now();

            return results;
        } finally {
            writeLock.unlock();
        }
    }

    public int getItemsReadyToFlush() {
        readLock.lock();
        try {
            if (bufferedItems.isEmpty()) {
                return 0;
            }

            Instant now = Instant.now();
            return (int) bufferedItems.stream()
                .filter(item -> {
                    Duration itemAge = Duration.between(item.getTimestamp(), now);
                    return itemAge.toNanos() >= config.getMaxItemAgeNanos();
                })
                .count();
        } finally {
            readLock.unlock();
        }
    }
}