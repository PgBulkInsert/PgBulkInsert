// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.util;

import de.bytefish.pgbulkinsert.exceptions.PgConnectionException;
import org.postgresql.PGConnection;
import org.postgresql.core.Utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

public final class PostgreSqlUtils {

    private PostgreSqlUtils() {
    }

    public static PGConnection getPGConnection(final Connection connection) {
        return tryGetPGConnection(connection).orElseThrow(() -> new PgConnectionException("Could not obtain a PGConnection"));
    }

    public static Optional<PGConnection> tryGetPGConnection(final Connection connection) {
        final Optional<PGConnection> fromCast = tryCastConnection(connection);
        if (fromCast.isPresent()) {
            return fromCast;
        }
        return tryUnwrapConnection(connection);
    }

    private static Optional<PGConnection> tryCastConnection(final Connection connection) {
        if (connection instanceof PGConnection) {
            return Optional.of((PGConnection) connection);
        }
        return Optional.empty();
    }

    private static Optional<PGConnection> tryUnwrapConnection(final Connection connection) {
        try {
            if (connection.isWrapperFor(PGConnection.class)) {
                return Optional.of(connection.unwrap(PGConnection.class));
            }
        } catch (Exception e) {
            // do nothing
        }
        return Optional.empty();
    }

    public static String quoteIdentifier(String identifier) {
        try {
            return Utils.escapeIdentifier(null, identifier).toString();
        } catch (SQLException e) {
            throw new IllegalArgumentException("Invalid identifier", e);
        }
    }

    public static String getFullyQualifiedTableName(String schemaName, String tableName, boolean usePostgresQuoting) {
        if (usePostgresQuoting) {
            return StringUtils.isNullOrWhiteSpace(schemaName) ? quoteIdentifier(tableName)
                    : String.format("%s.%s", quoteIdentifier(schemaName), quoteIdentifier(tableName));
        }

        if (StringUtils.isNullOrWhiteSpace(schemaName)) {
            return tableName;
        }

        return String.format("%1$s.%2$s", schemaName, tableName);
    }
}