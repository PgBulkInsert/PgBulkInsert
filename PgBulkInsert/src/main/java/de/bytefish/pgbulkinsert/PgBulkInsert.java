// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert;

import de.bytefish.pgbulkinsert.exceptions.SaveEntityFailedException;
import de.bytefish.pgbulkinsert.functional.Action2;
import de.bytefish.pgbulkinsert.functional.Func2;
import de.bytefish.pgbulkinsert.mapping.*;
import de.bytefish.pgbulkinsert.model.ColumnDefinition;
import de.bytefish.pgbulkinsert.model.TableDefinition;
import de.bytefish.pgbulkinsert.pgsql.PgBinaryWriter;
import de.bytefish.pgbulkinsert.pgsql.constants.DataType;
import de.bytefish.pgbulkinsert.pgsql.constants.ObjectIdentifier;
import de.bytefish.pgbulkinsert.pgsql.handlers.CollectionValueHandler;
import de.bytefish.pgbulkinsert.pgsql.handlers.IValueHandler;
import de.bytefish.pgbulkinsert.pgsql.handlers.IValueHandlerProvider;
import de.bytefish.pgbulkinsert.pgsql.handlers.ValueHandlerProvider;
import de.bytefish.pgbulkinsert.pgsql.model.geometric.*;
import de.bytefish.pgbulkinsert.pgsql.model.network.MacAddress;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.copy.PGCopyOutputStream;

import java.math.BigDecimal;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PgBulkInsert<TEntity> implements IPgBulkInsert<TEntity> {

    private final AbstractMapping<TEntity> mapping;

    public PgBulkInsert(AbstractMapping mapping)
    {
        this.mapping = mapping;
    }

    public void saveAll(PGConnection connection, Stream<TEntity> entities) throws SQLException {

        CopyManager cpManager = connection.getCopyAPI();
        CopyIn copyIn = cpManager.copyIn(mapping.getCopyCommand());

        try (PgBinaryWriter bw = new PgBinaryWriter()) {

            // Wrap the CopyOutputStream in our own Writer:
            bw.open(new PGCopyOutputStream(copyIn, 1));

            // Insert Each Column:
            entities.forEach(entity -> this.saveEntity(bw, entity));
        }
    }

    private void saveEntity(PgBinaryWriter bw, TEntity entity) throws SaveEntityFailedException {
        synchronized (bw) {
            // Start a New Row:
            bw.startRow(mapping.getColumns().size());

            // Iterate over each column mapping:
            mapping.getColumns().forEach(column -> {
                try {
                    column.getWrite().invoke(bw, entity);
                } catch (Exception e) {
                    throw new SaveEntityFailedException(e);
                }
            });
        }
    }
}
