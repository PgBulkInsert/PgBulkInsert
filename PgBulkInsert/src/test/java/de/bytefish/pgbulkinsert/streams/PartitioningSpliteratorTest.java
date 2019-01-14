package de.bytefish.pgbulkinsert.streams;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class PartitioningSpliteratorTest {

    @Test
    public void partitioningSpliteratorTest() throws ExecutionException, InterruptedException {
        int num = 100000;
        int partitionSize = 10;
        int batchSize = 10000;
        Stream<Collection<Integer>> s = StreamUtil.partition(IntStream.range(0, num).mapToObj(i -> i), partitionSize, batchSize);
        ForkJoinPool forkJoinPool = new ForkJoinPool(10);
        long sum = forkJoinPool.submit(() -> s.map(p -> {
            long count = p.stream().count();
            Assert.assertTrue(count == partitionSize);
            return count;
        }).reduce((a, b) -> a + b)).get().get();
        Assert.assertTrue(sum == num);
    }

}
