// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert;

import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class PgBulkInsertTest extends TransactionalTestBase {


    private class SampleEntity {

        public Integer columnInteger;
        public LocalDateTime columnDateTime;
        public Float columnFloat;
        public Double columnDouble;

        public SampleEntity(Integer columnInteger, LocalDateTime columnDateTime, Float columnFloat, Double columnDouble) {
            this.columnInteger = columnInteger;
            this.columnDateTime = columnDateTime;
            this.columnFloat = columnFloat;
            this.columnDouble = columnDouble;
        }

        public Integer getColumnInteger() {
            return columnInteger;
        }

        public void setColumnInteger(Integer columnInteger) {
            this.columnInteger = columnInteger;
        }

        public LocalDateTime getColumnDateTime() {
            return columnDateTime;
        }

        public void setColumnDateTime(LocalDateTime columnDateTime) {
            this.columnDateTime = columnDateTime;
        }

        public Float getColumnFloat() {
            return columnFloat;
        }

        public void setColumnFloat(Float columnFloat) {
            this.columnFloat = columnFloat;
        }

        public Double getColumnDouble() {
            return columnDouble;
        }

        public void setColumnDouble(Double columnDouble) {
            this.columnDouble = columnDouble;
        }
    }

    @Test
    public void saveAllTest() throws SQLException {
        createTable();

        List<SampleEntity> entities = new ArrayList<>();

        entities.add(new SampleEntity(1, null, null, null));

        PgBulkInsert<SampleEntity> pgBulkInsert = new PgBulkInsert<SampleEntity>("sample", "unit_test")
                .MapInt("col_integer", SampleEntity::getColumnInteger);

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while(rs.next()) {
            int v = rs.getInt(2);

            Assert.assertEquals(1, v);
        }
    }

    @Test
    public void saveAllTest2() throws SQLException {
        createTable();

        List<SampleEntity> entities = new ArrayList<>();

        entities.add(new SampleEntity(1, LocalDateTime.of(2010, 1, 1, 0, 0), null, null));

        PgBulkInsert<SampleEntity> pgBulkInsert = new PgBulkInsert<SampleEntity>("sample", "unit_test")
                .MapLocalDateTime("col_timestamp", SampleEntity::getColumnDateTime);

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while(rs.next()) {
            Timestamp v = rs.getTimestamp(5);

            Assert.assertEquals(LocalDateTime.of(2010, 1, 1, 0, 0, 0), v.toLocalDateTime());
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
                "                col_smallint smallint,\n" +
                "                col_integer integer,\n" +
                "                col_money money,\n" +
                "                col_bigint bigint,\n" +
                "                col_timestamp timestamp,\n" +
                "                col_real real,\n" +
                "                col_double double precision,\n" +
                "                col_bytea bytea,\n" +
                "                col_uuid uuid,\n" +
                "                col_numeric numeric,\n" +
                "                col_inet inet,\n" +
                "                col_macaddr macaddr,\n" +
                "                col_date date,\n" +
                "                col_interval interval\n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

}
