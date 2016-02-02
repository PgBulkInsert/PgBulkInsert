// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert;

import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class PgBulkInsertTest extends TransactionalTestBase {

    private class SampleEntity {

        private Integer col_integer;
        private LocalDateTime col_datetime;
        private Float col_float;
        private Double col_double;
        private BigInteger col_big_int;
        private String col_text;
        private Long col_long;
        private Short col_short;
        private UUID col_uuid;

        public Integer get_col_integer() {
            return col_integer;
        }

        public LocalDateTime get_col_datetime() {
            return col_datetime;
        }

        public Float get_col_float() {
            return col_float;
        }

        public Double get_col_double() {
            return col_double;
        }

        public BigInteger get_col_big_int() {
            return col_big_int;
        }

        public void set_col_integer(Integer col_integer) {
            this.col_integer = col_integer;
        }

        public void set_col_datetime(LocalDateTime col_datetime) {
            this.col_datetime = col_datetime;
        }

        public void set_col_float(Float col_float) {
            this.col_float = col_float;
        }

        public void set_col_double(Double col_double) {
            this.col_double = col_double;
        }

        public void set_col_big_int(BigInteger col_big_int) {
            this.col_big_int = col_big_int;
        }

        public String get_col_text() {
            return col_text;
        }

        public void set_col_text(String col_text) {
            this.col_text = col_text;
        }

        public Long get_col_long() {
            return col_long;
        }

        public void set_col_long(Long col_long) {
            this.col_long = col_long;
        }

        public Short get_col_short() {
            return col_short;
        }

        public void set_col_short(Short col_short) {
            this.col_short = col_short;
        }

        public UUID get_col_uuid() {
            return col_uuid;
        }

        public void set_col_uuid(UUID col_uuid) {
            this.col_uuid = col_uuid;
        }
    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Override
    protected void onSetUpBeforeTransaction() throws Exception {

    }

    @Test
    public void saveAll_Short_Test() throws SQLException {

        // This list will be inserted.
        List<SampleEntity> entities = new ArrayList<>();

        // Create the Entity to insert:
        SampleEntity entity = new SampleEntity();
        entity.col_short = 1;

        entities.add(entity);

        PgBulkInsert<SampleEntity> pgBulkInsert = new PgBulkInsert<SampleEntity>("sample", "unit_test")
                .MapShort("col_smallint", SampleEntity::get_col_short);

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while(rs.next()) {
            short v = rs.getShort("col_smallint");

            Assert.assertEquals(1, v);
        }
    }

    @Test
    public void saveAll_Integer_Test() throws SQLException {

        // This list will be inserted.
        List<SampleEntity> entities = new ArrayList<>();

        // Create the Entity to insert:
        SampleEntity entity = new SampleEntity();
        entity.col_integer = 1;

        entities.add(entity);

        PgBulkInsert<SampleEntity> pgBulkInsert = new PgBulkInsert<SampleEntity>("sample", "unit_test")
                .MapInt("col_integer", SampleEntity::get_col_integer);

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while(rs.next()) {
            int v = rs.getInt("col_integer");

            Assert.assertEquals(1, v);
        }
    }

    @Test
    public void saveAll_LocalDateTime_Test() throws SQLException {

        // This list will be inserted.
        List<SampleEntity> entities = new ArrayList<>();

        // Create the Entity to insert:
        SampleEntity entity = new SampleEntity();
        entity.col_datetime = LocalDateTime.of(2010, 1, 1, 0, 0);

        entities.add(entity);

        PgBulkInsert<SampleEntity> pgBulkInsert = new PgBulkInsert<SampleEntity>("sample", "unit_test")
                .MapLocalDateTime("col_timestamp", SampleEntity::get_col_datetime);

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while(rs.next()) {
            Timestamp v = rs.getTimestamp("col_timestamp");

            Assert.assertEquals(LocalDateTime.of(2010, 1, 1, 0, 0, 0), v.toLocalDateTime());
        }
    }

    @Test
    public void saveAll_String_Test() throws SQLException {

        // This list will be inserted.
        List<SampleEntity> entities = new ArrayList<>();

        // Create the Entity to insert:
        SampleEntity entity = new SampleEntity();
        entity.col_text = "ABC";

        entities.add(entity);

        PgBulkInsert<SampleEntity> pgBulkInsert = new PgBulkInsert<SampleEntity>("sample", "unit_test")
                .MapString("col_text", SampleEntity::get_col_text);

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while(rs.next()) {
            String v = rs.getString("col_text");

            Assert.assertEquals("ABC", v);
        }
    }


    @Test
    public void saveAll_BigInt_Test() throws SQLException {

        // This list will be inserted.
        List<SampleEntity> entities = new ArrayList<>();

        // Create the Entity to insert:
        SampleEntity entity = new SampleEntity();
        entity.col_big_int = new BigInteger("1");

        entities.add(entity);

        PgBulkInsert<SampleEntity> pgBulkInsert = new PgBulkInsert<SampleEntity>("sample", "unit_test")
                .MapBigInt("col_bigint", SampleEntity::get_col_big_int);

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while(rs.next()) {
            BigDecimal bd = rs.getBigDecimal("col_bigint");
            Assert.assertEquals(new BigInteger("1"), bd.toBigInteger());
        }
    }

    @Test
    public void saveAll_Long_Test() throws SQLException {

        // This list will be inserted.
        List<SampleEntity> entities = new ArrayList<>();

        // Create the Entity to insert:
        SampleEntity entity = new SampleEntity();
        entity.col_long = 1L;

        entities.add(entity);

        PgBulkInsert<SampleEntity> pgBulkInsert = new PgBulkInsert<SampleEntity>("sample", "unit_test")
                .MapLong("col_bigint", SampleEntity::get_col_long);

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while(rs.next()) {
            long v = rs.getLong("col_bigint");
            Assert.assertEquals(1, v);
        }
    }

    @Test
    public void saveAll_Multiple_Entities_Test() throws SQLException {

        // This list will be inserted.
        List<SampleEntity> entities = new ArrayList<>();

        // Create the Entities to insert:
        SampleEntity entity0 = new SampleEntity();
        entity0.col_long = 1L;

        SampleEntity entity1 = new SampleEntity();
        entity1.col_long = 2L;

        entities.add(entity0);
        entities.add(entity1);

        PgBulkInsert<SampleEntity> pgBulkInsert = new PgBulkInsert<SampleEntity>("sample", "unit_test")
                .MapLong("col_bigint", SampleEntity::get_col_long);

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        Assert.assertEquals(2, getRowCount());

        ResultSet rs = getAll();

        // Turn it into a List:
        ArrayList<Long> values = new ArrayList<>();

        while(rs.next()) {
            values.add(rs.getLong("col_bigint"));
        }

        Assert.assertTrue(values.stream().anyMatch(x -> x == 1));
        Assert.assertTrue(values.stream().anyMatch(x -> x == 2));
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
                "                col_interval interval,\n" +
                "                col_text text\n" +
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
