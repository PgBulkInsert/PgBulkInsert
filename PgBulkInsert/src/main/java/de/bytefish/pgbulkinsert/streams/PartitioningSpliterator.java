package de.bytefish.pgbulkinsert.streams;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

public class PartitioningSpliterator<T> extends BatchSpliterator<Collection<T>> {

    private final Spliterator<T> spliterator;
    private final int partitionSize;

    public PartitioningSpliterator(Spliterator<T> spliterator, int partitionSize, int batchSize) {
        super(batchSize, ORDERED | DISTINCT | NONNULL | IMMUTABLE);
        assert partitionSize > 0;
        this.spliterator = spliterator;
        this.partitionSize = partitionSize;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Collection<T>> consumer) {
        final HoldingConsumer holder = new HoldingConsumer();
        if (!spliterator.tryAdvance(holder)) {
            return false;
        }
        final List<T> partition = new ArrayList<>();
        int j = 0;
        do {
            partition.add((T) holder.getValue());
        } while (++j < partitionSize && spliterator.tryAdvance(holder));
        consumer.accept(partition);
        return true;
    }

}