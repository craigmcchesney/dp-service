package com.ospreydcs.dp.service.ingest.benchmark;

public class BenchmarkIngestDataStreamBytes extends BenchmarkIngestDataStream {

    public static void main(final String[] args) {
        BenchmarkIngestDataStreamBytes benchmark = new BenchmarkIngestDataStreamBytes();
        runBenchmark(benchmark, ColumnDataType.SERIALIZED_DATA_COLUMN);
    }

}
