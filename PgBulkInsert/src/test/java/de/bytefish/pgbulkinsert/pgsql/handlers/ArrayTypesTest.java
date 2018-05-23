// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.net.UnknownHostException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ArrayTypesTest  extends TransactionalTestBase {

    private class ArrayEntity {

        public List<String> stringArray;

        public List<String> getStringArray() {
            return stringArray;
        }
    }


    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Override
    protected void onSetUpBeforeTransaction() throws Exception {

    }

    private class ArrayEntityMapping extends AbstractMapping<ArrayEntity> {

        public ArrayEntityMapping() {
            super("sample", "unit_test");

            mapStringArray("col_string_array", ArrayEntity::getStringArray);
        }

    }


    @Test
    public void saveAll_StringArray_Test() throws SQLException, UnknownHostException {

        // This list will be inserted.
        List<ArrayEntity> entities = new ArrayList<>();

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();

        entity.stringArray = Arrays.asList("A", "B");

        entities.add(entity);

        PgBulkInsert<ArrayEntity> pgBulkInsert = new PgBulkInsert<>(new ArrayEntityMapping());

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            Array z = rs.getArray("col_string_array");

            String[] v = (String[]) z.getArray();

            Assert.assertEquals("A", v[0]);
            Assert.assertEquals("B", v[1]);
        }
    }

    private ResultSet getAll() throws SQLException {
        String sqlStatement = "SELECT * FROM sample.unit_test";

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }

    private boolean createTable() throws SQLException {
        String sqlStatement = "CREATE TABLE sample.unit_test\n" +
                "            (\n" +
                "                col_string_array text[]\n" +
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
