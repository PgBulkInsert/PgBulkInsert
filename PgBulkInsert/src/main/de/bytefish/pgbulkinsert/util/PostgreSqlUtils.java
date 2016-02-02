// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.util;

import com.sun.istack.internal.NotNull;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.exceptions.PgConnectionException;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.SQLException;

public final class PostgreSqlUtils {

    private PostgreSqlUtils() {
    }

    public static PGConnection getPGConnection(final Connection connection) throws SQLException {
        OutParameter<PGConnection> result = new OutParameter<>();
        if(!tryGetPGConnection(connection, result)) {
            throw new PgConnectionException("Could not obtain a PGConnection");
        }
        return result.get();
    }

    public static boolean tryGetPGConnection(final Connection connection, @NotNull OutParameter<PGConnection> result) throws SQLException {
        if(tryCastConnection(connection, result)) {
            return true;
        }
        if(tryUnwrapConnection(connection, result)) {
            return true;
        }
        return false;
    }

    private static boolean tryCastConnection(final Connection connection, @NotNull OutParameter<PGConnection> result) {
        if (connection instanceof PGConnection) {
            result.set((PGConnection) connection);

            return true;
        }
        return false;
    }

    private static boolean tryUnwrapConnection(final Connection connection, @NotNull OutParameter<PGConnection> result) {
        try {
            if (connection.isWrapperFor(PGConnection.class)) {
                result.set(connection.unwrap(PGConnection.class));
                return true;
            }
            return false;
        } catch(Exception e) {
            return false;
        }
    }
}