package it.damore.solr.backuprestore;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collector;

public class StreamUtils {

    /**
     * Creates a new batch collector
     * @param batchSize the batch size after which the batchProcessor should be called
     * @param batchProcessor the batch processor which accepts batches of records to process
     * @param <T> the type of elements being processed
     * @return a batch collector instance
     */
    public static <T> Collector<T, List<T>, List<T>> batchCollector(int batchSize, Consumer<List<T>> batchProcessor) {
        return new BatchCollector<T>(batchSize, batchProcessor);
    }
}