// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.integration;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

// https://github.com/bytefish/PgBulkInsert/issues/23
public class Issue23DuplicatesTest extends TransactionalTestBase {

    private class MyObject {
        private final int pos;
        private final String name;
        private final String descr;
        private final String type;
        private final String otherProps;

        public MyObject(int pos, String name, String descr, String type, String otherProps) {
            this.pos = pos;
            this.name = name;
            this.descr = descr;
            this.type = type;
            this.otherProps = otherProps;
        }

        public int getPos() {
            return pos;
        }

        public String getName() {
            return name;
        }

        public String getDescr() {
            return descr;
        }

        public String getType() {
            return type;
        }

        public String getOtherProps() {
            return otherProps;
        }
    }

    private class MyObjectMapper extends AbstractMapping<MyObject> {

        public MyObjectMapper() {
            super(schema, "unit_test");

            mapInteger("pos", MyObject::getPos);
            mapText("name", MyObject::getName);
            mapText("descr", MyObject::getDescr);
            mapText("type", MyObject::getType);
            mapText("otherProps", MyObject::getOtherProps);
        }
    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Test
    public void bulkInsertDataTest() throws SQLException {

        List<MyObject> testData = Arrays.asList(
                new MyObject(1, "vector_db", "xxxxxx", "postgis (JNDI)", "unique prop1"),
                new MyObject(2, "vector_db", "xxxxxx", "postgis (JNDI)", "unique prop2"),
                new MyObject(3, "vector_db", "xxxxxx", "postgis (JNDI)", "unique prop3"),
                new MyObject(4, "client_db", "xxxxxx", "postgis (JNDI)", "unique prop")
        );

        PgBulkInsert<MyObject> writer = new PgBulkInsert<MyObject>(new MyObjectMapper());

        writer.saveAll(PostgreSqlUtils.getPGConnection(connection), testData.stream());

        // And assert all have been written to the database:
        Assert.assertEquals(4, getRowCount());

        compareData(testData);
    }


    private boolean createTable() throws SQLException {

        String sqlStatement = String.format("CREATE TABLE %s.unit_test\n", schema) +
                "            (\n" +
                "                pos int,\n" +
                "                name text,\n" +
                "                descr text,\n" +
                "                type text,\n" +
                "                otherProps text\n" +
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

    private void compareData(List<MyObject> testData) throws SQLException {

        Statement s = connection.createStatement();

        ResultSet resultSet = s.executeQuery(String.format("select * from %s.unit_test order by pos asc", schema));

        int pos = 0;
        while (resultSet.next()) {
            MyObject original = testData.get(pos);

            Assert.assertEquals(resultSet.getString("name"), original.getName());
            Assert.assertEquals(resultSet.getString("descr"), original.getDescr());
            Assert.assertEquals(resultSet.getString("type"), original.getType());
            Assert.assertEquals(resultSet.getString("otherProps"), original.getOtherProps());

            pos++;
        }
    }

}
