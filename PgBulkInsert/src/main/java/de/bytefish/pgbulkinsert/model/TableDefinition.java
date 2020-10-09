// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.model;

import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.util.StringUtils;

public class TableDefinition {

    private final String schema;

    private final String tableName;

    public TableDefinition(String tableName) {
        this("", tableName);
    }

    public TableDefinition(String schema, String tableName) {
        this.schema = schema;
        this.tableName = tableName;
    }

    public String getSchema() {
        return schema;
    }

    public String getTableName() {
        return tableName;
    }

    public String GetFullyQualifiedTableName(boolean usePostgresQuoting) {
        return PostgreSqlUtils.getFullyQualifiedTableName(schema, tableName, usePostgresQuoting);
    }

    @Override
    public String toString() {
        return String.format("TableDefinition (Schema = {%1$s}, TableName = {%2$s})", schema, tableName);
    }
}