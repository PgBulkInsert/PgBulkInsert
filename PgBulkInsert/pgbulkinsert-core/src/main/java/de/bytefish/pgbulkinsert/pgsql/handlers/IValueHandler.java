// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.DataOutputStream;

public interface IValueHandler<TTargetType> extends ValueHandler {

    void handle(DataOutputStream buffer, @Nullable final TTargetType value);

    int getLength(final TTargetType value);
}
