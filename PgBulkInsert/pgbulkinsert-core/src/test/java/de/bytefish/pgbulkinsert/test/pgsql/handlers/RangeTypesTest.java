// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.test.pgsql.handlers;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.pgsql.model.range.Range;
import de.bytefish.pgbulkinsert.test.utils.TransactionalTestBase;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PGobject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class RangeTypesTest extends TransactionalTestBase {

    private class RangeEntity {
        public Range<ZonedDateTime> timeTzRange;
        public Range<LocalDateTime> timeRange;
        public Range<Integer> int4Range;
        public Range<Long> int8Range;
        public Range<Number> numericRange;
        public Range<LocalDate> dateRange;
    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Override
    protected void onSetUpBeforeTransaction() throws Exception {

    }

    private class RangeEntityMapping extends AbstractMapping<RangeEntity> {

        public RangeEntityMapping() {
            super(schema, "time_table");

            mapTsTzRange("col_tstzrange", (x) -> x.timeTzRange);
            mapTsRange("col_tsrange", (x) -> x.timeRange);
            mapInt4Range("col_int4range", (x) -> x.int4Range);
            mapInt8Range("col_int8range", (x) -> x.int8Range);
            mapNumRange("col_numrange", (x) -> x.numericRange);
            mapDateRange("col_daterange", (x) -> x.dateRange);
        }
    }

    private boolean createTable() throws SQLException {
        String sqlStatement = String.format("CREATE TABLE %s.time_table(\n", schema)
                + "  col_tstzrange tstzrange"
                + ",  col_tsrange tsrange"
                + ",  col_int4range int4range"
                + ",  col_int8range int8range"
                + ",  col_numrange numrange"
                + ",  col_daterange daterange"
                + ");";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    @Test
    public void test_SaveTsRange_Inclusive_Bounds() throws SQLException {

        // This list will be inserted.
        List<RangeEntity> entities = new ArrayList<>();

        // Range to insert:
        RangeEntity entity0 = new RangeEntity();

        LocalDateTime lower = LocalDateTime.of(2020, 1, 1, 0, 0, 0 ,0);
        LocalDateTime upper = LocalDateTime.of(2020, 3, 1, 0, 0, 0 ,0);

        entity0.timeRange = new Range<>(lower, upper);

        entities.add(entity0);

        // Construct the Insert:
        PgBulkInsert<RangeEntity> bulkInsert = new PgBulkInsert<>(new RangeEntityMapping());

        // Save them:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGobject v0 = (PGobject) rs.getObject("col_tsrange");

            Assert.assertEquals("[\"2020-01-01 00:00:00\",\"2020-03-01 00:00:00\"]", v0.getValue());
        }
    }

    @Test
    public void test_SaveDateRange_Inclusive_Bounds() throws SQLException {

        // This list will be inserted.
        List<RangeEntity> entities = new ArrayList<>();

        // Range to insert:
        RangeEntity entity0 = new RangeEntity();

        LocalDate lower = LocalDate.of(2020, 1, 1);
        LocalDate upper = LocalDate.of(2020, 3, 1);

        entity0.dateRange = new Range<>(lower, upper);

        entities.add(entity0);

        // Construct the Insert:
        PgBulkInsert<RangeEntity> bulkInsert = new PgBulkInsert<>(new RangeEntityMapping());

        // Save them:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGobject v0 = (PGobject) rs.getObject("col_daterange");

            // Why [2020-01-01,2020-03-02)?
            //
            // https://www.postgresql.org/docs/9.3/rangetypes.html:
            //
            //  The built-in range types int4range, int8range, and daterange all use a canonical form
            //  that includes the lower bound and excludes the upper bound; that is, [). User-defined
            //  range types can use other conventions, however.
            Assert.assertEquals("[2020-01-01,2020-03-02)", v0.getValue());
        }
    }

    @Test
    public void test_SaveInt4Range_Inclusive_Bounds() throws SQLException {

        // This list will be inserted.
        List<RangeEntity> entities = new ArrayList<>();

        // Range to insert:
        RangeEntity entity0 = new RangeEntity();

        int lower = 1;
        int upper = 8;

        entity0.int4Range = new Range<Integer>(lower, upper);

        entities.add(entity0);

        // Construct the Insert:
        PgBulkInsert<RangeEntity> bulkInsert = new PgBulkInsert<>(new RangeEntityMapping());

        // Save them:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGobject v0 = (PGobject) rs.getObject("col_int4range");

            // Why is it [1,9):
            //
            // https://www.postgresql.org/docs/9.3/rangetypes.html:
            //
            //  The built-in range types int4range, int8range, and daterange all use a canonical form
            //  that includes the lower bound and excludes the upper bound; that is, [). User-defined
            //  range types can use other conventions, however.
            Assert.assertEquals("[1,9)", v0.getValue());
        }
    }

    @Test
    public void test_SaveInt8Range_Inclusive_Bounds() throws SQLException {

        // This list will be inserted.
        List<RangeEntity> entities = new ArrayList<>();

        // Range to insert:
        RangeEntity entity0 = new RangeEntity();

        long lower = 1;
        long upper = 8;

        entity0.int8Range = new Range<Long>(lower, upper);

        entities.add(entity0);

        // Construct the Insert:
        PgBulkInsert<RangeEntity> bulkInsert = new PgBulkInsert<>(new RangeEntityMapping());

        // Save them:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGobject v0 = (PGobject) rs.getObject("col_int8range");

            // Why is it [1,9):
            //
            // https://www.postgresql.org/docs/9.3/rangetypes.html:
            //
            //  The built-in range types int4range, int8range, and daterange all use a canonical form
            //  that includes the lower bound and excludes the upper bound; that is, [). User-defined
            //  range types can use other conventions, however.

            Assert.assertEquals("[1,9)", v0.getValue());
        }
    }

    @Test
    public void test_SaveNumericRange_Inclusive_Bounds() throws SQLException {

        // This list will be inserted.
        List<RangeEntity> entities = new ArrayList<>();

        // Range to insert:
        RangeEntity entity0 = new RangeEntity();

        double lower = 1.2;
        double upper = 8.2;

        entity0.numericRange = new Range<>(lower, upper);

        entities.add(entity0);

        // Construct the Insert:
        PgBulkInsert<RangeEntity> bulkInsert = new PgBulkInsert<>(new RangeEntityMapping());

        // Save them:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGobject v0 = (PGobject) rs.getObject("col_numrange");

            // Why is it [1,9):
            //
            // https://www.postgresql.org/docs/9.3/rangetypes.html:
            //
            //  The built-in range types int4range, int8range, and daterange all use a canonical form
            //  that includes the lower bound and excludes the upper bound; that is, [). User-defined
            //  range types can use other conventions, however.

            Assert.assertEquals("[1.2,8.2]", v0.getValue());
        }
    }

    @Test
    public void test_SaveTsTzRange_Inclusive_Bounds() throws SQLException {

        // This list will be inserted.
        List<RangeEntity> entities = new ArrayList<>();

        // Range to insert:
        RangeEntity entity0 = new RangeEntity();

        ZonedDateTime lower = ZonedDateTime.of(2020, 1, 1, 0, 0, 0 ,0,  ZoneId.of("GMT"));
        ZonedDateTime upper = ZonedDateTime.of(2020, 3, 1, 0, 0, 0 ,0,  ZoneId.of("GMT"));

        entity0.timeTzRange = new Range<>(lower, upper);

        entities.add(entity0);

        // Construct the Insert:
        PgBulkInsert<RangeEntity> bulkInsert = new PgBulkInsert<>(new RangeEntityMapping());

        // Save them:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGobject v0 = (PGobject) rs.getObject("col_tstzrange");

            Assert.assertEquals("[\"2020-01-01 01:00:00+01\",\"2020-03-01 01:00:00+01\"]", v0.getValue());
        }
    }

    @Test
    public void test_SaveTsTzRange_UpperBound_Null() throws SQLException {

        // This list will be inserted.
        List<RangeEntity> entities = new ArrayList<>();

        // Range to insert:
        RangeEntity entity0 = new RangeEntity();

        ZonedDateTime lower = ZonedDateTime.of(2020, 1, 1, 0, 0, 0 ,0,  ZoneId.of("GMT"));
        ZonedDateTime upper = null;

        entity0.timeTzRange = new Range<>(lower, upper);

        entities.add(entity0);

        // Construct the Insert:
        PgBulkInsert<RangeEntity> bulkInsert = new PgBulkInsert<>(new RangeEntityMapping());

        // Save them:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGobject v0 = (PGobject) rs.getObject("col_tstzrange");

            Assert.assertEquals("[\"2020-01-01 01:00:00+01\",)", v0.getValue());
        }
    }

    @Test
    public void test_SaveTsTzRange_LowerBound_Null() throws SQLException {

        // This list will be inserted.
        List<RangeEntity> entities = new ArrayList<>();

        // Range to insert:
        RangeEntity entity0 = new RangeEntity();

        ZonedDateTime lower = null;
        ZonedDateTime upper = ZonedDateTime.of(2020, 1, 1, 0, 0, 0 ,0,  ZoneId.of("GMT"));

        entity0.timeTzRange = new Range<>(lower, upper);

        entities.add(entity0);

        // Construct the Insert:
        PgBulkInsert<RangeEntity> bulkInsert = new PgBulkInsert<>(new RangeEntityMapping());

        // Save them:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGobject v0 = (PGobject) rs.getObject("col_tstzrange");

            Assert.assertEquals("(,\"2020-01-01 01:00:00+01\"]", v0.getValue());
        }
    }

    @Test
    public void test_SaveTsTzRange_Empty() throws SQLException {

        // This list will be inserted.
        List<RangeEntity> entities = new ArrayList<>();

        // Range to insert:
        RangeEntity entity0 = new RangeEntity();

        ZonedDateTime lower = null;
        ZonedDateTime upper = null;

        entity0.timeTzRange = new Range<>(lower, upper);

        entities.add(entity0);

        // Construct the Insert:
        PgBulkInsert<RangeEntity> bulkInsert = new PgBulkInsert<>(new RangeEntityMapping());

        // Save them:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGobject v0 = (PGobject)rs.getObject("col_tstzrange");

            Assert.assertEquals("(,)", v0.getValue());
        }
    }

    private ResultSet getAll() throws SQLException {
        String sqlStatement = String.format("SELECT * FROM %s.time_table", schema);

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }



}
