// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert;

import org.postgresql.PGConnection;

import java.sql.SQLException;
import java.util.stream.Stream;

public interface IPgBulkInsert<TEntity> {

    void saveAll(PGConnection connection, Stream<TEntity> entities) throws SQLException;

}
