// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.util;


public class ExceptionUtils {

    private ExceptionUtils() {}

    public static Throwable getRootCause(Throwable t) {
        if (t == null) {
            return null;
        }

        Throwable rootCause = null;
        Throwable cause = t.getCause();

        // Now get to the Inner-Most Cause:
        while (cause != null && cause != rootCause) {
            rootCause = cause;
            cause = cause.getCause();
        }

        return rootCause;
    }
}
