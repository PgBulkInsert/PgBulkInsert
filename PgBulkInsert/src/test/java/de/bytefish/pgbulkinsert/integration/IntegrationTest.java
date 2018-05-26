// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.integration;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.PersonMapping;
import de.bytefish.pgbulkinsert.model.Person;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class IntegrationTest extends TransactionalTestBase {

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

@Test
public void bulkInsertPersonDataTest() throws SQLException {
    // Create a large list of Persons:
    List<Person> persons = getPersonList(100000);
    // Create the BulkInserter:
    PgBulkInsert<Person> bulkInsert = new PgBulkInsert<Person>(new PersonMapping());
    // Now save all entities of a given stream:
    bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), persons.stream());
    // And assert all have been written to the database:
    Assert.assertEquals(100000, getRowCount());
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

        String sqlStatement = String.format("CREATE TABLE %s.unit_test\n", schema) +
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

        ResultSet r = s.executeQuery(String.format("SELECT COUNT(*) AS rowcount FROM %s.unit_test", schema));
        r.next();
        int count = r.getInt("rowcount");
        r.close();

        return count;
    }

}
