// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import java.math.BigDecimal;
import java.net.UnknownHostException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;

public class NumericArrayTypesTest extends TransactionalTestBase {

    private class ArrayEntity {

        public List<BigDecimal> bigDecimalArray;
        public List<Double> doubleArray;
        public List<Float> floatArray;
        public List<Long> longArray;
        public List<Short> shortArray;
        public List<Integer> integerArray;

        public List<BigDecimal> getBigDecimalArray() { return bigDecimalArray; }
        public List<Double> getDoubleArray() { return doubleArray; }
        public List<Float> getFloatArray() { return floatArray; }
        public List<Long> getLongArray() { return longArray; }
        public List<Short> getShortArray() { return shortArray; }
        public List<Integer> getIntegerArray() { return integerArray; }
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
            super(SCHEMA, "unit_test");

            mapNumericArray("col_numeric_array", ArrayEntity::getBigDecimalArray);
            mapNumericArray("col_double_array", ArrayEntity::getDoubleArray);
            mapNumericArray("col_float_array", ArrayEntity::getFloatArray);
            mapNumericArray("col_long_array", ArrayEntity::getLongArray);
            mapNumericArray("col_short_array", ArrayEntity::getShortArray);
            mapNumericArray("col_integer_array", ArrayEntity::getIntegerArray);
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

        testArrayInternal("col_numeric_array", entity, entity.bigDecimalArray, x -> x);
    }


    @Test
    public void saveAll_DoubleArray_Test() throws SQLException, UnknownHostException {

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();
        entity.doubleArray = Arrays.asList(
                new Double("210000.00011234567"),
                new Double("310000.00011234567")
        );

        testArrayInternal("col_double_array", entity, entity.doubleArray, BigDecimal::doubleValue);
    }

    @Test
    public void saveAll_FloatArray_Test() throws SQLException, UnknownHostException {

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();
        entity.floatArray = Arrays.asList(
                new Float("210000.00011234567"),
                new Float("310000.00011234567")
        );

        testArrayInternal("col_float_array", entity, entity.floatArray, BigDecimal::floatValue);
    }

    @Test
    public void saveAll_LongArray_Test() throws SQLException, UnknownHostException {

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();
        entity.longArray = Arrays.asList(
                new Long("211234"),
                new Long("4534534")
        );

        testArrayInternal("col_long_array", entity, entity.longArray, BigDecimal::longValue);
    }

    @Test
    public void saveAll_ShortArray_Test() throws SQLException, UnknownHostException {

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();
        entity.shortArray = Arrays.asList(
                new Short("42"),
                new Short("34")
        );

        testArrayInternal("col_short_array", entity, entity.shortArray, BigDecimal::shortValue);
    }

    @Test
    public void saveAll_IntegerArray_Test() throws SQLException, UnknownHostException {

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();
        entity.integerArray = Arrays.asList(
                new Integer("3453455"),
                new Integer("5435345")
        );

        testArrayInternal("col_integer_array", entity, entity.integerArray, BigDecimal::intValue);
    }

    private <T> void testArrayInternal(String columnLabel, ArrayEntity entity, List<T> samples, Function<BigDecimal, T> converter) throws SQLException, UnknownHostException {

        List<ArrayEntity> entities = Collections.singletonList(entity);

        PgBulkInsert<ArrayEntity> pgBulkInsert = new PgBulkInsert<>(new ArrayEntityMapping());

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            Array z = rs.getArray(columnLabel);

            BigDecimal[] v = (BigDecimal[]) z.getArray();

            for (int i=0; i<samples.size(); i++) {

                T element = converter.apply(v[i]);

                Assert.assertEquals(samples.get(i), element);
            }
        }
    }

    private ResultSet getAll() throws SQLException {
        String sqlStatement = String.format("SELECT * FROM %s.unit_test", SCHEMA);

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }

    private boolean createTable() throws SQLException {
        String sqlStatement = String.format("CREATE TABLE %s.unit_test\n", SCHEMA) +
                "            (\n" +
                "                col_numeric_array numeric[],\n" +
                "                col_double_array numeric[],\n" +
                "                col_float_array numeric[],\n" +
                "                col_long_array numeric[],\n" +
                "                col_short_array numeric[],\n" +
                "                col_integer_array numeric[],\n" +
                "                col_boolean_array numeric[]\n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    private int getRowCount() throws SQLException {

        Statement s = connection.createStatement();

        ResultSet r = s.executeQuery(String.format("SELECT COUNT(*) AS rowcount FROM %s.unit_test", SCHEMA));
        r.next();
        int count = r.getInt("rowcount");
        r.close();

        return count;
    }

}
