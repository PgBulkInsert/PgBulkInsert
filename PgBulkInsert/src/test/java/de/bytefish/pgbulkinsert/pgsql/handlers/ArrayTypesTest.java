// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.pgsql.constants.DataType;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
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

        public List<BigDecimal> numericArray;

        public List<BigDecimal> getNumericArray() {
            return numericArray;
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
            super(schema, "unit_test");

            mapCollection("col_string_array", DataType.VarChar, ArrayEntity::getStringArray);
            mapCollection("col_numeric_array", DataType.Numeric, ArrayEntity::getNumericArray);
        }

    }

    @Test
    public void saveAll_NumericArray_Test() throws SQLException, UnknownHostException {

        // This list will be inserted.
        List<ArrayEntity> entities = new ArrayList<>();

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();

        entity.numericArray = Arrays.asList(
                new BigDecimal("210000.00011234567"),
                new BigDecimal("310000.00011234567")
        );

        entities.add(entity);

        PgBulkInsert<ArrayEntity> pgBulkInsert = new PgBulkInsert<>(new ArrayEntityMapping());

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            Array z = rs.getArray("col_numeric_array");

            BigDecimal[] v = (BigDecimal[]) z.getArray();

            Assert.assertEquals(new BigDecimal("210000.00011234567"), v[0].stripTrailingZeros());
            Assert.assertEquals(new BigDecimal("310000.00011234567"), v[1].stripTrailingZeros());
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
        String sqlStatement = String.format("SELECT * FROM %s.unit_test", schema);

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }

    private boolean createTable() throws SQLException {
        String sqlStatement = String.format("CREATE TABLE %s.unit_test\n", schema) +
                "            (\n" +
                "                col_string_array varchar[], \n" +
                "                col_numeric_array numeric[]\n" +
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
