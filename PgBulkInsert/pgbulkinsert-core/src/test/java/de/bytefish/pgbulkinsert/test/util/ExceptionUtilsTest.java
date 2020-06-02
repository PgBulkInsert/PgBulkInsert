// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.test.util;

import de.bytefish.pgbulkinsert.exceptions.BinaryWriteFailedException;
import de.bytefish.pgbulkinsert.util.ExceptionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

public class ExceptionUtilsTest {

    @Test
    public void testExceptionUtils() {

        // Exception Hierarchy:
        SQLException sqlException = new SQLException("My SQLException");
        IOException ioException = new IOException("My IOException with a SQLException cause", sqlException);
        BinaryWriteFailedException binaryWriteFailedException = new BinaryWriteFailedException(ioException);

        // Get the root cause:
        Throwable innerMostException = ExceptionUtils.getRootCause(binaryWriteFailedException);

        Assert.assertNotNull(innerMostException);
        Assert.assertEquals("My SQLException", innerMostException.getMessage());
    }
}
