package de.bytefish.pgbulkinsert.util;

import org.junit.Assert;
import org.junit.Test;

public class PostgresUtilsTest {

    @Test
    public void testUnquotedIdentifiers() {
        final String identifier = "binary";
        final String result = PostgreSqlUtils.quoteIdentifier(identifier);

        Assert.assertEquals("\"binary\"", result);
    }

    @Test
    public void testAlreadyQuotedIdentifier() {
        final String identifier = "\"binary\"";
        final String result = PostgreSqlUtils.quoteIdentifier(identifier);

        Assert.assertEquals("\"binary\"", result);
    }
}
