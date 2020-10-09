// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.util;

import org.checkerframework.checker.nullness.qual.Nullable;

public class ExceptionUtils {

    private ExceptionUtils() {}

    @Nullable
    public static Throwable getRootCause(@Nullable Throwable t) {
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
