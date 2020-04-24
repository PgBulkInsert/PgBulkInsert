package de.bytefish.pgbulkinsert.test.row;

import de.bytefish.pgbulkinsert.pgsql.model.range.Range;
import de.bytefish.pgbulkinsert.row.SimpleRowWriter;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.PGConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class SimpleRowWriterTest extends TransactionalTestBase {

    private static final String tableName = "row_writer_test";

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Test
    public void rowBasedWriterTest() throws SQLException {

        // Get the underlying PGConnection:
        PGConnection pgConnection = PostgreSqlUtils.getPGConnection(connection);

        // Define the Columns to be inserted:
        String[] columnNames = new String[] {
                "value_int",
                "value_text",
                "value_range"
        };

        // Create the Table Definition:
        SimpleRowWriter.Table table = new SimpleRowWriter.Table(schema, tableName, columnNames);

        // Create the Writer:
        SimpleRowWriter writer = new SimpleRowWriter(table);

        // ... open it:
        writer.open(pgConnection);

        // ... write your data rows:
        for(int rowIdx = 0; rowIdx < 10000; rowIdx++) {

            // ... using startRow and work with the row, see how the order doesn't matter:
            writer.startRow((row) -> {
                row.setText("value_text", "Hi");
                row.setInteger("value_int", 1);
                row.setTsTzRange("value_range", new Range<>(
                        ZonedDateTime.of(2020, 3, 1, 0, 0, 0, 0, ZoneId.of("GMT")),
                        ZonedDateTime.of(2020, 3, 1, 0, 0, 0, 0, ZoneId.of("GMT"))));
            });
        }

        // ... and make sure to close it:
        writer.close();

        // Now assert, that we have written 10000 entities:

        Assert.assertEquals(10000, getRowCount());
    }

    private boolean createTable() throws SQLException {

        String sqlStatement = String.format("CREATE TABLE %s.%s\n", schema, tableName) +
                "            (\n" +
                "                value_int int\n"+
                ",                value_text text\n" +
                ",                value_range tstzrange\n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    private int getRowCount() throws SQLException {

        Statement s = connection.createStatement();

        ResultSet r = s.executeQuery(String.format("SELECT COUNT(*) AS rowcount FROM %s.%s", schema, tableName));
        r.next();
        int count = r.getInt("rowcount");
        r.close();

        return count;
    }
}
