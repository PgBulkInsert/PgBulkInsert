// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert;

import de.bytefish.pgbulkinsert.configuration.Configuration;
import de.bytefish.pgbulkinsert.configuration.IConfiguration;
import de.bytefish.pgbulkinsert.exceptions.SaveEntityFailedException;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.pgsql.PgBinaryWriter;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

public class PgBulkInsert<TEntity> implements IPgBulkInsert<TEntity> {

    private final IConfiguration configuration;
    private final AbstractMapping<TEntity> mapping;

    public PgBulkInsert(AbstractMapping<TEntity> mapping) {
        this(new Configuration(), mapping);
    }

    public PgBulkInsert(IConfiguration configuration, AbstractMapping<TEntity> mapping)
    {
        Objects.requireNonNull(configuration, "'configuration' has to be set");
        Objects.requireNonNull(mapping, "'mapping' has to be set");

        this.configuration = configuration;
        this.mapping = mapping;
    }

    @Override
    public void saveAll(PGConnection connection, Stream<TEntity> entities) throws SQLException {
        // Wrap the CopyOutputStream in our own Writer:
        try (PgBinaryWriter bw = new PgBinaryWriter(new PGCopyOutputStream(connection, mapping.getCopyCommand(), 1), configuration.getBufferSize())) {
            // Insert Each Column:
            entities.forEach(entity -> saveEntitySynchonized(bw, entity));
        }
    }

    public void saveAll(PGConnection connection, Collection<TEntity> entities) throws SQLException {
        saveAll(connection, entities.stream());
    }

    private void saveEntity(PgBinaryWriter bw, TEntity entity) throws SaveEntityFailedException {
        // Start a new Row in PostgreSQL:
        bw.startRow(mapping.getColumns().size());

        try {
	        // Iterate over each column mapping:
	        mapping.getColumns().forEach(column -> {
	        	column.getWrite().accept(bw, entity);
	        });
        } catch (Exception e) {
            throw new SaveEntityFailedException(e);
        }
    }

    private void saveEntitySynchonized(PgBinaryWriter bw, TEntity entity) throws SaveEntityFailedException {
        synchronized (bw) {
        	saveEntity(bw, entity);
        }
    }
}