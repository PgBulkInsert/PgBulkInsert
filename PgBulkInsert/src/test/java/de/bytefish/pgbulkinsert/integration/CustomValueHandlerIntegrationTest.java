// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.integration;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.pgsql.constants.DataType;
import de.bytefish.pgbulkinsert.pgsql.handlers.BaseValueHandler;
import de.bytefish.pgbulkinsert.pgsql.handlers.BigDecimalValueHandler;
import de.bytefish.pgbulkinsert.pgsql.handlers.IValueHandler;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class CustomValueHandlerIntegrationTest extends TransactionalTestBase {

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    private class DoubleNumericValueHandler extends BaseValueHandler<Double> {

        private IValueHandler<BigDecimal> bigDecimalIValueHandler;

        public DoubleNumericValueHandler() {
            this(new BigDecimalValueHandler());
        }

        public DoubleNumericValueHandler(IValueHandler<BigDecimal> bigDecimalValueHandler) {
            this.bigDecimalIValueHandler = bigDecimalValueHandler;
        }

        @Override
        public DataType getDataType() {
            return DataType.Numeric;
        }

        @Override
        protected void internalHandle(DataOutputStream buffer, Double value) throws Exception {
            BigDecimal decimal = BigDecimal.valueOf(value);

            bigDecimalIValueHandler.handle(buffer, decimal);
        }
    }

    private class SampleEntity {

        private Double doubleValue;

        public SampleEntity() {

        }

        public Double getDoubleValue() {
            return doubleValue;
        }

        public void setDoubleValue(Double value) {
            doubleValue = value;
        }
    }

    public class CustomValueHandlerMapping extends AbstractMapping<SampleEntity> {

        public CustomValueHandlerMapping() {
            super("sample", "unit_test");

            map("numeric_column", new DoubleNumericValueHandler(), SampleEntity::getDoubleValue);
        }
    }

    @Test
    public void writeNumericTest() throws Exception {

        // This list will be inserted.
        List<SampleEntity> entities = new ArrayList<>();

        // Create the Entity to insert:
        SampleEntity entity = new SampleEntity();

        entity.setDoubleValue(21.124);

        entities.add(entity);

        CustomValueHandlerMapping mapping = new CustomValueHandlerMapping();
        PgBulkInsert<SampleEntity> bulkInsert = new PgBulkInsert<>(mapping);

        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            BigDecimal v = rs.getBigDecimal("numeric_column");


            Assert.assertEquals(new BigDecimal("21.124"), v.stripTrailingZeros());
        }
    }

    private boolean createTable() throws SQLException {

        String sqlStatement = "CREATE TABLE sample.unit_test\n" +
                "            (\n" +
                "                numeric_column numeric(20, 10)\n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    private ResultSet getAll() throws SQLException {
        String sqlStatement = "SELECT * FROM sample.unit_test";

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }
}
