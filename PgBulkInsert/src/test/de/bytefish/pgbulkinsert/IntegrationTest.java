// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert;

import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class IntegrationTest extends TransactionalTestBase {

    private class Person {

        private String firstName;

        private String lastName;

        private LocalDate birthDate;

        public Person() {}

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        public LocalDate getBirthDate() {
            return birthDate;
        }

        public void setBirthDate(LocalDate birthDate) {
            this.birthDate = birthDate;
        }

    }

    public class PersonBulkInserter extends PgBulkInsert<Person>
    {
        public PersonBulkInserter() {
            super("sample", "unit_test");

            mapString("first_name", Person::getFirstName);
            mapString("last_name", Person::getLastName);
            mapDate("birth_date", Person::getBirthDate);
        }
    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Test
    public void bulkInsertPersonDataTest() throws SQLException {
        // Create a large list of Persons:
        List<Person> persons = getPersonList(100000);
        // Create the BulkInserter:
        PersonBulkInserter personBulkInserter = new PersonBulkInserter();
        // Now save all entities of a given stream:
        personBulkInserter.saveAll(PostgreSqlUtils.getPGConnection(connection), persons.stream());
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
