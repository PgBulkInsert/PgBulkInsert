package de.bytefish.pgbulkinsert.test.util;

import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
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
    public void testOneDoubleQuote() {
        final String identifier = "\"";
        final String result = PostgreSqlUtils.quoteIdentifier(identifier);

        Assert.assertEquals("\"\"\"\"", result);
    }

    @Test
    public void testTwoDoubleQuotes() {
        final String identifier = "\"\"";
        final String result = PostgreSqlUtils.quoteIdentifier(identifier);

        Assert.assertEquals("\"\"\"\"\"\"", result);
    }

    @Test
    public void testIdentifierWithQuotes() {
        final String identifier = "\"x\"";
        final String result = PostgreSqlUtils.quoteIdentifier(identifier);

        Assert.assertEquals("\"\"\"x\"\"\"", result);
    }

    @Test
    public void testIdentifierWithQuoteInTheMiddle() {
        final String identifier = "x\"y";
        final String result = PostgreSqlUtils.quoteIdentifier(identifier);

        Assert.assertEquals("\"x\"\"y\"", result);
    }
}
