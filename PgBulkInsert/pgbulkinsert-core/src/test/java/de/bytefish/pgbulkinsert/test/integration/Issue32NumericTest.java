// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.test.integration;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// https://github.com/bytefish/PgBulkInsert/issues/23
public class Issue32NumericTest extends TransactionalTestBase {

    private class MyObject {

        private final int idx;
        private final BigDecimal bigDecimal;

        private MyObject(int idx, BigDecimal bigDecimal) {
            this.idx = idx;
            this.bigDecimal = bigDecimal;
        }

        public BigDecimal getBigDecimal() {
            return bigDecimal;
        }

        public int getIdx() {
            return idx;
        }
    }

    private class MyObjectMapper extends AbstractMapping<MyObject> {

        public MyObjectMapper() {
            super(schema, "issue32");

            mapNumeric("bigDecimal", MyObject::getBigDecimal);
        }
    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Test
    public void bulkInsertDataTest() throws SQLException {

        List<MyObject> testData = Arrays.asList(
                new MyObject(0, new BigDecimal("0.0")),
                new MyObject(1, new BigDecimal("110.022004090000")),
                new MyObject(2, new BigDecimal("-0.0")),
                new MyObject(3, new BigDecimal("-110.022004090000")),

                new MyObject(4, new BigDecimal("-1100.12345")),
                new MyObject(5, new BigDecimal("-1100.1234")),
                new MyObject(6, new BigDecimal("-1100.123")),
                new MyObject(7, new BigDecimal("-1100.12")),
                new MyObject(8, new BigDecimal("-1100.1")),
                new MyObject(9, new BigDecimal("-1100")),

                new MyObject(10, new BigDecimal("0.01")),
                new MyObject(11, new BigDecimal("0.001")),
                new MyObject(12, new BigDecimal("0.0001000")),

                new MyObject(13, new BigDecimal("0000.000")),
                new MyObject(14, new BigDecimal("000.000")),
                new MyObject(15, new BigDecimal("00.000")),

                new MyObject(16, new BigDecimal("-12345.12345")),
                new MyObject(17, new BigDecimal("-1234.12345")),
                new MyObject(18, new BigDecimal("-123.12345")),
                new MyObject(19, new BigDecimal("-12.12345")),
                new MyObject(20, new BigDecimal("-1.12345"))
        );

        PgBulkInsert<MyObject> writer = new PgBulkInsert<MyObject>(new MyObjectMapper());

        writer.saveAll(PostgreSqlUtils.getPGConnection(connection), testData.stream());

        // And assert all have been written to the database:
        Assert.assertEquals(21, getRowCount());

        ArrayList<BigDecimal> bigDecimals = getBigDecimals();

        Assert.assertEquals(new BigDecimal("0.0").stripTrailingZeros(), bigDecimals.get(0).stripTrailingZeros());
        Assert.assertEquals(new BigDecimal("110.022004090000").stripTrailingZeros(), bigDecimals.get(1).stripTrailingZeros());
        Assert.assertEquals(new BigDecimal("0.0").stripTrailingZeros(), bigDecimals.get(2).stripTrailingZeros());
        Assert.assertEquals(new BigDecimal("-110.022004090000").stripTrailingZeros(), bigDecimals.get(3).stripTrailingZeros());

        Assert.assertEquals(new BigDecimal("-1100.12345").stripTrailingZeros(), bigDecimals.get(4).stripTrailingZeros());
        Assert.assertEquals(new BigDecimal("-1100.1234").stripTrailingZeros(), bigDecimals.get(5).stripTrailingZeros());
        Assert.assertEquals(new BigDecimal("-1100.123").stripTrailingZeros(), bigDecimals.get(6).stripTrailingZeros());
        Assert.assertEquals(new BigDecimal("-1100.12").stripTrailingZeros(), bigDecimals.get(7).stripTrailingZeros());
        Assert.assertEquals(new BigDecimal("-1100.1").stripTrailingZeros(), bigDecimals.get(8).stripTrailingZeros());
        Assert.assertEquals(new BigDecimal("-1100").stripTrailingZeros(), bigDecimals.get(9).stripTrailingZeros());

        Assert.assertEquals(new BigDecimal("0.01").stripTrailingZeros(), bigDecimals.get(10).stripTrailingZeros());
        Assert.assertEquals(new BigDecimal("0.001").stripTrailingZeros(), bigDecimals.get(11).stripTrailingZeros());
        Assert.assertEquals(new BigDecimal("0.0001000").stripTrailingZeros(), bigDecimals.get(12).stripTrailingZeros());


        Assert.assertEquals(new BigDecimal("0000.000").stripTrailingZeros(), bigDecimals.get(13).stripTrailingZeros());
        Assert.assertEquals(new BigDecimal("000.000").stripTrailingZeros(), bigDecimals.get(14).stripTrailingZeros());
        Assert.assertEquals(new BigDecimal("00.000").stripTrailingZeros(), bigDecimals.get(15).stripTrailingZeros());

        Assert.assertEquals(new BigDecimal("-12345.12345").stripTrailingZeros(), bigDecimals.get(16).stripTrailingZeros());
        Assert.assertEquals(new BigDecimal("-1234.12345").stripTrailingZeros(), bigDecimals.get(17).stripTrailingZeros());
        Assert.assertEquals(new BigDecimal("-123.12345").stripTrailingZeros(), bigDecimals.get(18).stripTrailingZeros());
        Assert.assertEquals(new BigDecimal("-12.12345").stripTrailingZeros(), bigDecimals.get(19).stripTrailingZeros());
        Assert.assertEquals(new BigDecimal("-1.12345").stripTrailingZeros(), bigDecimals.get(20).stripTrailingZeros());
    }

    private boolean createTable() throws SQLException {

        String sqlStatement = String.format("CREATE TABLE %s.issue32\n", schema) +
                "            (\n" +
                "                idx int,\n"+
                "                bigDecimal NUMERIC(100, 14)\n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    private int getRowCount() throws SQLException {

        Statement s = connection.createStatement();

        ResultSet r = s.executeQuery(String.format("SELECT COUNT(*) AS rowcount FROM %s.issue32", schema));
        r.next();
        int count = r.getInt("rowcount");
        r.close();

        return count;
    }

    private ArrayList<BigDecimal> getBigDecimals() throws SQLException {

        ResultSet rs = getAll();

        ArrayList<BigDecimal> results = new ArrayList<>();

        while (rs.next()) {
            BigDecimal z = rs.getBigDecimal("bigDecimal");

            results.add(z);
        }

        return results;
    }

    private ResultSet getAll() throws SQLException {
        String sqlStatement = String.format("SELECT * FROM %s.issue32 order by idx asc", schema);

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }

}
