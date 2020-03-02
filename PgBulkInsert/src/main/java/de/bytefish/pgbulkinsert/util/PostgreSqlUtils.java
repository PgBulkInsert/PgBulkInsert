// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.util;

import de.bytefish.pgbulkinsert.exceptions.PgConnectionException;
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

    public static boolean tryGetPGConnection(final Connection connection, OutParameter<PGConnection> result) throws SQLException {
        if(tryCastConnection(connection, result)) {
            return true;
        }
        if(tryUnwrapConnection(connection, result)) {
            return true;
        }
        return false;
    }

    private static boolean tryCastConnection(final Connection connection, OutParameter<PGConnection> result) {
        if (connection instanceof PGConnection) {
            result.set((PGConnection) connection);

            return true;
        }
        return false;
    }

    private static boolean tryUnwrapConnection(final Connection connection, OutParameter<PGConnection> result) {
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

    public static final char QuoteChar = '"';

    public static String quoteIdentifier(String identifier) {
        return requiresQuoting(identifier) ?
                (QuoteChar + identifier + QuoteChar) : identifier;
    }

    public static String getFullyQualifiedTableName(String schemaName, String tableName, boolean usePostgresQuoting)
    {
        if(usePostgresQuoting) {
            return StringUtils.isNullOrWhiteSpace(schemaName) ? quoteIdentifier(tableName)
                    : String.format("%s.%s", quoteIdentifier(schemaName), quoteIdentifier(tableName));
        }

        if (StringUtils.isNullOrWhiteSpace(schemaName)) {
            return tableName;
        }

        return String.format("%1$s.%2$s", schemaName, tableName);
    }

    private static boolean requiresQuoting(String identifier) {

        char first = identifier.charAt(0);
        char last = identifier.charAt(identifier.length() - 1);

        if (first == QuoteChar && last == QuoteChar)
        {
            return false;
        }

        if (!Character.isLowerCase(first) && first != '_')
        {
            return true;
        }

        for (int i = 1; i < identifier.length(); i++)
        {
            char c = identifier.charAt(i);

            if (Character.isLowerCase(c))
            {
                continue;
            }

            switch (c)
            {
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                case '_':
                case '$': // yes it's true
                    continue;
            }

            return true;
        }

        return false;
    }
}