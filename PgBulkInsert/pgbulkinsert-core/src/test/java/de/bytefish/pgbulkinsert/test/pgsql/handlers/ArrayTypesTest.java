// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.test.pgsql.handlers;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.test.utils.TransactionalTestBase;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.net.UnknownHostException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ArrayTypesTest  extends TransactionalTestBase {

    private class ArrayEntity {

        public List<String> stringArray;
        public List<BigDecimal> bigDecimalArray;
        public List<Double> doubleArray;
        public List<Float> floatArray;
        public List<Long> longArray;
        public List<Short> shortArray;
        public List<Integer> integerArray;
        public List<Boolean> booleanArray;

        public List<String> getStringArray() {
            return stringArray;
        }
        public List<BigDecimal> getBigDecimalArray() { return bigDecimalArray; }
        public List<Double> getDoubleArray() { return doubleArray; }
        public List<Float> getFloatArray() { return floatArray; }
        public List<Long> getLongArray() { return longArray; }
        public List<Short> getShortArray() { return shortArray; }
        public List<Integer> getIntegerArray() { return integerArray; }
        public List<Boolean> getBooleanArray() { return booleanArray; }
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

            mapVarCharArray("col_varchar_array", ArrayEntity::getStringArray);
            mapTextArray("col_text_array", ArrayEntity::getStringArray);
            mapNumericArray("col_numeric_array", ArrayEntity::getBigDecimalArray);
            mapDoubleArray("col_double_array", ArrayEntity::getDoubleArray);
            mapFloatArray("col_float_array", ArrayEntity::getFloatArray);
            mapLongArray("col_long_array", ArrayEntity::getLongArray);
            mapShortArray("col_short_array", ArrayEntity::getShortArray);
            mapIntegerArray("col_integer_array", ArrayEntity::getIntegerArray);
            mapBooleanArray("col_boolean_array", ArrayEntity::getBooleanArray);
        }
    }

    @Test
    public void saveAll_NumericArray_Test() throws SQLException, UnknownHostException {

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();

        entity.bigDecimalArray = Arrays.asList(
                new BigDecimal("210000.00011234567"),
                new BigDecimal("310000.00011234567")
        );

        testArrayInternal("col_numeric_array", entity, entity.bigDecimalArray);
    }

    @Test
    public void saveAll_VarCharArray_Test() throws SQLException, UnknownHostException {

        testStringArray("col_varchar_array");
    }

    @Test
    public void saveAll_TextArray_Test() throws SQLException, UnknownHostException {

        testStringArray("col_text_array");
    }

    private void testStringArray(String columnLabel) throws SQLException, UnknownHostException {

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();
        entity.stringArray = Arrays.asList("A", "B");

        testArrayInternal(columnLabel, entity, entity.stringArray);
    }


    @Test
    public void saveAll_DoubleArray_Test() throws SQLException, UnknownHostException {

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();
        entity.doubleArray = Arrays.asList(
                new Double("210000.00011234567"),
                new Double("310000.00011234567")
        );

        testArrayInternal("col_double_array", entity, entity.doubleArray);
    }

    @Test
    public void saveAll_FloatArray_Test() throws SQLException, UnknownHostException {

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();
        entity.floatArray = Arrays.asList(
                new Float("210000.00011234567"),
                new Float("310000.00011234567")
        );

        testArrayInternal("col_float_array", entity, entity.floatArray);
    }

    @Test
    public void saveAll_LongArray_Test() throws SQLException, UnknownHostException {

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();
        entity.longArray = Arrays.asList(
                new Long("211234"),
                new Long("4534534")
        );

        testArrayInternal("col_long_array", entity, entity.longArray);
    }

    @Test
    public void saveAll_ShortArray_Test() throws SQLException, UnknownHostException {

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();
        entity.shortArray = Arrays.asList(
                new Short("42"),
                new Short("34")
        );

        testArrayInternal("col_short_array", entity, entity.shortArray);
    }

    @Test
    public void saveAll_IntegerArray_Test() throws SQLException, UnknownHostException {

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();
        entity.integerArray = Arrays.asList(
                new Integer("3453455"),
                new Integer("5435345")
        );

        testArrayInternal("col_integer_array", entity, entity.integerArray);
    }

    @Test
    public void saveAll_BooleanArray_Test() throws SQLException, UnknownHostException {

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();
        entity.booleanArray = Arrays.asList(
                Boolean.TRUE,
                Boolean.FALSE
        );

        testArrayInternal("col_boolean_array", entity, entity.booleanArray);
    }

    @SuppressWarnings("unchecked")
    private <T> void testArrayInternal(String columnLabel, ArrayEntity entity, List<T> samples) throws SQLException, UnknownHostException {
        List<ArrayEntity> entities = Collections.singletonList(entity);

        PgBulkInsert<ArrayEntity> pgBulkInsert = new PgBulkInsert<>(new ArrayEntityMapping());

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            Array z = rs.getArray(columnLabel);

            T[] v = (T[]) z.getArray();

            for (int i=0; i<samples.size(); i++) {
                Assert.assertEquals(samples.get(i), v[i]);
            }
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
                "                col_varchar_array varchar[], \n" +
                "                col_text_array text[], \n" +
                "                col_numeric_array numeric[],\n" +
                "                col_double_array double precision[],\n" +
                "                col_float_array real[],\n" +
                "                col_long_array int8[],\n" +
                "                col_short_array int2[],\n" +
                "                col_integer_array int4[],\n" +
                "                col_boolean_array boolean[]\n" +
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
