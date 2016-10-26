package it.damore.solr.backuprestore;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collector;

/*
This file is part of solr-backup-restore-json.

solr-backup-restore-json is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

solr-backup-restore-json is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with solr-backup-restore-json.  If not, see <http://www.gnu.org/licenses/>.
*/


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