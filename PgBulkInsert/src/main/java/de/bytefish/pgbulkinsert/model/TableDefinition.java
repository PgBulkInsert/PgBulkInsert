// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.model;

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

    public String GetFullQualifiedTableName() {
        if (StringUtils.isNullOrWhiteSpace(schema)) {
            return tableName;
        }
        return String.format("%1$s.%2$s", schema, tableName);
    }

    @Override
    public String toString() {
        return String.format("TableDefinition (Schema = {%1$s}, TableName = {%2$s})", schema, tableName);
    }
}