// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.processor;

import de.bytefish.pgbulkinsert.functional.Func1;
import de.bytefish.pgbulkinsert.mapping.PersonBulkInserter;
import de.bytefish.pgbulkinsert.model.Person;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.PGConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class BulkProcessorTest extends TransactionalTestBase {

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Test
    public void testAdd() throws Exception {
        // Create the BulkInserter to be wrapped:
        PersonBulkInserter personBulkInserter = new PersonBulkInserter();
        // Create the ConnectionFactory:
        Func1<PGConnection> connectionFactory = () -> PostgreSqlUtils.getPGConnection(connection);
        // Create the BulkProcessor:
        BulkProcessor<Person> bulkProcessor = new BulkProcessor<>(personBulkInserter, connectionFactory, 10);
        // Create some Test data:
        List<Person> fiftyPersons = getPersonList(50);
        // Now process them with the BulkProcessor:
        for (Person p : fiftyPersons) {
            bulkProcessor.add(p);
        }
        // The Processor should have fired 5 times, so 50 persons have been added:

        Assert.assertEquals(50, getRowCount());
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