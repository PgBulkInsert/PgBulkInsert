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
import java.util.Objects;
import java.util.function.Function;

public class NumericArrayTypesTest extends TransactionalTestBase {

    private static class ArrayEntity {

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
            super(schema, "unit_test");

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
                Double.parseDouble("210000.00011234567"),
                Double.parseDouble("310000.00011234567")
        );

        testArrayInternal("col_double_array", entity, entity.doubleArray, BigDecimal::doubleValue);
    }

    @Test
    public void saveAll_FloatArray_Test() throws SQLException, UnknownHostException {

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();
        entity.floatArray = Arrays.asList(
                Float.parseFloat("210000.00011234567"),
                Float.parseFloat("310000.00011234567")
        );

        testArrayInternal("col_float_array", entity, entity.floatArray, BigDecimal::floatValue);
    }

    @Test
    public void saveAll_LongArray_Test() throws SQLException, UnknownHostException {

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();
        entity.longArray = Arrays.asList(
                Long.parseLong("211234"),
                Long.parseLong("4534534")
        );

        testArrayInternal("col_long_array", entity, entity.longArray, BigDecimal::longValue);
    }

    @Test
    public void saveAll_ShortArray_Test() throws SQLException, UnknownHostException {

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();
        entity.shortArray = Arrays.asList(
                Short.parseShort("42"),
                Short.parseShort("34")
        );

        testArrayInternal("col_short_array", entity, entity.shortArray, BigDecimal::shortValue);
    }

    @Test
    public void saveAll_IntegerArray_Test() throws SQLException, UnknownHostException {

        // Create the Entity to insert:
        ArrayEntity entity = new ArrayEntity();
        entity.integerArray = Arrays.asList(
                Integer.parseInt("3453455"),
                Integer.parseInt("5435345")
        );

        testArrayInternal("col_integer_array", entity, entity.integerArray, BigDecimal::intValue);
    }

    private <T> void testArrayInternal(String columnLabel, ArrayEntity entity, List<T> samples, Function<BigDecimal, T> converter) throws SQLException {
        Objects.requireNonNull(samples, "samples");

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
        String sqlStatement = String.format("SELECT * FROM %s.unit_test", schema);

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }

    private boolean createTable() throws SQLException {
        String sqlStatement = String.format("CREATE TABLE %s.unit_test\n", schema) +
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
}
