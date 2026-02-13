package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.ingest.model.SourceMonitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SourceMonitorManager {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final Map<String, List<SourceMonitor>> subscriptionMap = new HashMap<>();
    public final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();


    public boolean init() {
        return true;
    }

    public boolean fini() {

        logger.debug("SourceMonitorManager fini");

        if (shutdownRequested.compareAndSet(false, true)) {

            // only acquire readLock to access local data structure, release before shutting down the monitors
            readLock.lock();

            Set<SourceMonitor> sourceMonitors = null;
            try {

                logger.debug("SourceMonitorManager fini shutting down SourceMonitors");

                // create a set of all SourceMonitors, eliminating duplicates for subscriptions to multiple PVs
                sourceMonitors = new HashSet<>();
                for (List<SourceMonitor> monitorList : subscriptionMap.values()) {
                    sourceMonitors.addAll(monitorList);
                }

            } finally {
                // release the lock after accessing data structure but before shutting down monitors
                readLock.unlock();
            }
            Objects.requireNonNull(sourceMonitors);

            // after releasing lock, close each response stream in set
            for (SourceMonitor monitor : sourceMonitors) {
                monitor.requestShutdown();
            }

        }

        return true;
    }

    /**
     * Add a subscription entry to map data structure.  We use a write lock for thread safety between calling threads
     * (e.g., threads handling registration of subscriptions).
     */
    public void addMonitor(SourceMonitor monitor) {

        if (shutdownRequested.get()) {
            return;
        }

        writeLock.lock();
        try {
            // use try...finally to make sure we unlock

            for (String pvName : monitor.pvNames) {
                List<SourceMonitor> sourceMonitors = subscriptionMap.computeIfAbsent(pvName, k -> new ArrayList<>());
                sourceMonitors.add(monitor);
            }

        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Send a response to subscribers of PVs contained in this ingestion request.  We use a read lock for thread safety
     * between calling threads (e.g., workers processing ingestion requests).
     * @param request
     */
    public void publishDataSubscriptions(IngestDataRequest request, Set<String> requestPvs) {

        if (shutdownRequested.get()) {
            return;
        }

        // iterate through request PVs, publish request data to subscribers
        for (String pvName : requestPvs) {

            // acquire readLock only long enough to read local data structure with subscription monitors for specified PV
            readLock.lock();
            List<SourceMonitor> sourceMonitorsCopy;
            try {
                final List<SourceMonitor> sourceMonitors = subscriptionMap.get(pvName);
                sourceMonitorsCopy =
                        (sourceMonitors == null) ? new ArrayList<>() : new ArrayList<>(sourceMonitors);
            } finally {
                readLock.unlock();
            }

            // publish request data to subscribers for PV
            for (SourceMonitor monitor : sourceMonitorsCopy) {
                // publish data to subscriber if response stream is active
                monitor.publishDataFrame(pvName, request.getIngestionDataFrame());
            }
        }
    }

    /**
     * Remove all subscriptions from map for specified SourceMonitor, and then request shutdown.
     * We use a write lock for thread safety between calling threads
     * (e.g., threads handling registration of subscriptions).
     */
    public void removeMonitor(SourceMonitor monitor) {

        if (shutdownRequested.get()) {
            return;
        }

        writeLock.lock();
        try {
            // use try...finally to make sure we unlock

            for (String pvName : monitor.pvNames) {
                List<SourceMonitor> sourceMonitors = subscriptionMap.get(pvName);
                if (sourceMonitors != null) {
                    logger.debug(
                            "removing subscription for id: {} pv: {}",
                            monitor.responseObserver.hashCode(), pvName);
                    sourceMonitors.remove(monitor);
                }
            }

        } finally {
            writeLock.unlock();
        }
    }

    public void terminateMonitor(SourceMonitor monitor) {

        if (shutdownRequested.get()) {
            return;
        }

        logger.debug("terminateMonitor id: {}", monitor.responseObserver.hashCode());

        // terminate the SourceMonitor first (before acquiring any locks)
        monitor.requestShutdown();

        // then remove SourceMonitor from local data structures
        this.removeMonitor(monitor);
    }
}
