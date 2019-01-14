package de.bytefish.pgbulkinsert.streams;

import java.util.Comparator;
import java.util.Spliterator;

import static java.util.Spliterators.spliterator;

public abstract class BatchSpliterator <T> implements Spliterator<T> {

    private final int batchSize;

    private final int characteristics;

    public BatchSpliterator(int batchSize, int characteristics) {
        this.batchSize = batchSize;
        this.characteristics = characteristics | SUBSIZED;
    }

    @Override
    public Spliterator<T> trySplit() {
        final HoldingConsumer consumer = new HoldingConsumer();
        if (!tryAdvance(consumer)) {
            return null;
        }
        final Object[] batch = new Object[batchSize];
        int j = 0;
        do {
            batch[j] = consumer.getValue();
        } while (++j < batchSize && tryAdvance(consumer));
        return spliterator(batch, 0, j, characteristics() | SIZED);
    }

    @Override
    public Comparator<? super T> getComparator() {
        if (hasCharacteristics(SORTED)) {
            return null;
        }
        throw new IllegalStateException();
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return characteristics;
    }

}
