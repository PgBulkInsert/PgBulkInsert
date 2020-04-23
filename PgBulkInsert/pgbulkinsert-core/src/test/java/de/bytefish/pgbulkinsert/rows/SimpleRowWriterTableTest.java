package de.bytefish.pgbulkinsert.rows;

import de.bytefish.pgbulkinsert.row.SimpleRowWriter;
import org.junit.Assert;
import org.junit.Test;

public class SimpleRowWriterTableTest {

    @Test
    public void testGetCopyCommandFromTableWithQuoting() {
        String schemaName = "user";
        String tableName = "binary";
        String[] columns = new String[] { "id", "binary", "text" };

        SimpleRowWriter.Table table = new SimpleRowWriter.Table(schemaName, tableName, columns);

        String res = table.getCopyCommand(true);

        Assert.assertEquals("COPY \"user\".\"binary\"(\"id\", \"binary\", \"text\") FROM STDIN BINARY", res);
    }

    @Test
    public void testGetCopyCommandFromTableWithoutQuoting() {
        String schemaName = "user";
        String tableName = "binary";
        String[] columns = new String[] { "id", "binary", "text" };

        SimpleRowWriter.Table table = new SimpleRowWriter.Table(schemaName, tableName, columns);

        String res = table.getCopyCommand(false);

        Assert.assertEquals("COPY user.binary(id, binary, text) FROM STDIN BINARY", res);
    }


}
