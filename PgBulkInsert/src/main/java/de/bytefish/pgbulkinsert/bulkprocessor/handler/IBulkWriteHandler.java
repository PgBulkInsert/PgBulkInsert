// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.bulkprocessor.handler;

import java.util.List;

public interface IBulkWriteHandler<TEntity> {

    void write(List<TEntity> entities) throws Exception;

}
