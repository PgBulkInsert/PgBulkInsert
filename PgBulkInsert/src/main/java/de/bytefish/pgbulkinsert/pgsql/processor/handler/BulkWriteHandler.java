// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.processor.handler;

import de.bytefish.pgbulkinsert.IPgBulkInsert;
import de.bytefish.pgbulkinsert.functional.Func1;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.util.List;

public class BulkWriteHandler<TEntity> implements IBulkWriteHandler<TEntity> {

    private final IPgBulkInsert<TEntity> client;

    private final Func1<Connection> connectionFactory;

    public BulkWriteHandler(IPgBulkInsert<TEntity> client, Func1<Connection> connectionFactory) {
        this.client = client;
        this.connectionFactory = connectionFactory;
    }

    public void write(List<TEntity> entities) throws Exception {
        // Obtain a new Connection and execute it in a try with resources block, so it gets closed properly:
        try(Connection connection = connectionFactory.invoke()) {
            // Now get the underlying PGConnection for the COPY API wrapping:
            final PGConnection pgConnection = PostgreSqlUtils.getPGConnection(connection);
            // And finally save all entities by using the COPY API:
            client.saveAll(pgConnection, entities.stream());
        }
    }
}
