package de.bytefish.pgbulkinsert.streams;

import java.util.Collection;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamUtil {

    public static <T> Stream<Collection<T>> partition(Stream<T> stream, int partitionSize, int batchSize) {
        Spliterator<Collection<T>> spliterator = new PartitioningSpliterator<T>(stream.spliterator(), partitionSize, batchSize);
        return StreamSupport.stream(spliterator, true);
    }

}
