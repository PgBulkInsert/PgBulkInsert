package de.bytefish.pgbulkinsert.test.util;

import de.bytefish.pgbulkinsert.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class StringUtilsTest {

    @Test
    public void testRemoveNullCharacter() {
        final String textIncludingNullCharacter = "This is a\0 valid UTF8 String!";
        final String result = StringUtils.removeNullCharacter(textIncludingNullCharacter);

        Assert.assertEquals("This is a valid UTF8 String!", result);
    }
}
