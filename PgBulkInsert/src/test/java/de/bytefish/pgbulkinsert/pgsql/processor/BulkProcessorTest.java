// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.processor;

import de.bytefish.pgbulkinsert.IPgBulkInsert;
import de.bytefish.pgbulkinsert.functional.Func1;
import de.bytefish.pgbulkinsert.mapping.PersonBulkInserter;
import de.bytefish.pgbulkinsert.model.Person;
import de.bytefish.pgbulkinsert.pgsql.processor.handler.BulkWriteHandler;
import de.bytefish.pgbulkinsert.pgsql.processor.handler.IBulkWriteHandler;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class BulkProcessorTest extends TransactionalTestBase {

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    // Define a Custom Handler for the Unit Test, which does not close the Connection:
    class CustomBulkWriteHandler<TEntity> implements IBulkWriteHandler<TEntity> {

        private final IPgBulkInsert<TEntity> client;
        private final Func1<Connection> connectionFactory;

        public CustomBulkWriteHandler(IPgBulkInsert<TEntity> client, Func1<Connection> connectionFactory) {
            this.client = client;
            this.connectionFactory = connectionFactory;
        }

        public void write(List<TEntity> entities) throws Exception {
            Connection connection = connectionFactory.invoke();

            client.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());
        }
    }

    @Test
    public void testAdd() throws Exception {
        // Create the BulkInserter to be wrapped:
        IPgBulkInsert<Person> personBulkInserter = new PersonBulkInserter();
        // Create the ConnectionFactory:
        Func1<Connection> connectionFactory = () -> connection;
        // Create the BulkHandler:
        IBulkWriteHandler<Person> bulkWriteHandler = new CustomBulkWriteHandler<>(personBulkInserter, connectionFactory);
        // Create the BulkProcessor:
        BulkProcessor<Person> bulkProcessor = new BulkProcessor<>(bulkWriteHandler, 10, Duration.ofSeconds(1));
        // Create some Test data:
        List<Person> fiftyPersons = getPersonList(50);
        // Now process them with the BulkProcessor:
        for (Person p : fiftyPersons) {
            bulkProcessor.add(p);
        }
        // The Processor should have fired 5 times, so 50 persons have been added:

        Assert.assertEquals(50, getRowCount());
    }

    @Test
    public void testAddTimeBased() throws Exception {
        // Create the BulkInserter to be wrapped:
        IPgBulkInsert<Person> personBulkInserter = new PersonBulkInserter();
        // Create the ConnectionFactory:
        Func1<Connection> connectionFactory = () -> connection;
        // Create the BulkHandler:
        IBulkWriteHandler<Person> bulkWriteHandler = new CustomBulkWriteHandler<>(personBulkInserter, connectionFactory);
        // Create the BulkProcessor:
        BulkProcessor<Person> bulkProcessor = new BulkProcessor<>(bulkWriteHandler, 10, Duration.ofSeconds(1));
        // Create some Test data:
        List<Person> threePersons = getPersonList(3);
        // Now process them with the BulkProcessor:
        for (Person p : threePersons) {
            bulkProcessor.add(p);
        }
        // Sleep for 2 Seconds:
        Thread.sleep(Duration.ofSeconds(2).toMillis());

        // The three items should have been added:
        Assert.assertEquals(3, getRowCount());
    }

    @Test
    public void testWriteOnClose() throws Exception {
        // Create the BulkInserter to be wrapped:
        IPgBulkInsert<Person> personBulkInserter = new PersonBulkInserter();
        // Create the ConnectionFactory:
        Func1<Connection> connectionFactory = () -> connection;
        // Create the BulkHandler:
        IBulkWriteHandler<Person> bulkWriteHandler = new CustomBulkWriteHandler<>(personBulkInserter, connectionFactory);
        // Create the BulkProcessor:
        BulkProcessor<Person> bulkProcessor = new BulkProcessor<>(bulkWriteHandler, 10, Duration.ofSeconds(1));
        // Create some Test data:
        List<Person> threePersons = getPersonList(3);
        // Now process them with the BulkProcessor:
        for (Person p : threePersons) {
            bulkProcessor.add(p);
        }
        // Close the processor:
        bulkProcessor.close();
        // The three items should have been added:
        Assert.assertEquals(3, getRowCount());
    }

    private List<Person> getPersonList(int numPersons) {
        List<Person> persons = new ArrayList<>();

        for (int pos = 0; pos < numPersons; pos++) {
            Person p = new Person();

            p.setFirstName("Philipp");
            p.setLastName("Wagner");
            p.setBirthDate(LocalDate.of(1986, 5, 12));

            persons.add(p);
        }

        return persons;
    }

    private boolean createTable() throws SQLException {

        String sqlStatement = "CREATE TABLE sample.unit_test\n" +
                "            (\n" +
                "                first_name text,\n" +
                "                last_name text,\n" +
                "                birth_date date\n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    private int getRowCount() throws SQLException {

        Statement s = connection.createStatement();

        ResultSet r = s.executeQuery("SELECT COUNT(*) AS rowcount FROM sample.unit_test");
        r.next();
        int count = r.getInt("rowcount");
        r.close();

        return count;
    }
}