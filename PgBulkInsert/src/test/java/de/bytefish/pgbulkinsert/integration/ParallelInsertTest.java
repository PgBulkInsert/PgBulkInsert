// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.integration;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.PGConnection;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ParallelInsertTest {

    private class MyObject {

        private final int idx;

        private MyObject(int idx) {
            this.idx = idx;
        }

        public int getIdx() {
            return idx;
        }
    }

    private class MyObjectMapper extends AbstractMapping<MyObject> {

        public MyObjectMapper() {
            super("sample", "parallel_inserts");

            mapInteger("idx", MyObject::getIdx);
        }
    }

    @Before
    public void setUp() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/sampledb", "philipp", "test_pwd")) {
            dropTable(connection);
            createTable(connection);
        } catch (Exception e) {
            e.printStackTrace();
        }
        ;
    }


    @Test
    public void bulkInsertDataTest() throws SQLException {

        // Create the Writer to use for Bulk Writes:
        PgBulkInsert<MyObject> writer = new PgBulkInsert<MyObject>(new MyObjectMapper());

        // Create a Fake Data Stream:
        Stream<MyObject> stream = IntStream
                .range(0, 100000)
                .mapToObj(i -> new MyObject(i));

        // Partition the Stream into 10000 Element Batches:
        UnmodifiableIterator<List<MyObject>> batches = Iterators.partition(stream.iterator(), 10000);

        // Create the Batch Spliterator, probably it can be improved?
        Spliterator<List<MyObject>> batchSpliterator = Spliterators.spliteratorUnknownSize(batches, Spliterator.ORDERED);

        // Build a Parallel Stream using the Spliterator:
        Stream<List<MyObject>> parallelBatchesStream = StreamSupport.stream(batchSpliterator, true);

        // Now use the Batching Iterator from Guava to insert in Parallel:
        parallelBatchesStream
                .forEach(batch -> {
                    // Always open up a new Connection:
                    try (Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/sampledb", "philipp", "test_pwd")) {
                        // Cast into the underlying PGConnection:
                        PGConnection pgConnection = PostgreSqlUtils.getPGConnection(connection);
                        // And save it to DB:
                        writer.saveAll(pgConnection, batch);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
    }


    private boolean dropTable(Connection connection) throws SQLException {
        String sqlStatement = String.format("DROP TABLE IF EXISTS %s.parallel_inserts\n", "sample");

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    private boolean createTable(Connection connection) throws SQLException {
        String sqlStatement = String.format("CREATE TABLE %s.parallel_inserts\n", "sample") +
                "            (\n" +
                "                idx int\n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }
}
