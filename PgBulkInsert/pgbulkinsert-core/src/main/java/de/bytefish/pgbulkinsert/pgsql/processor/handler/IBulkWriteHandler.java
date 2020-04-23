// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.processor.handler;

import java.util.List;

public interface IBulkWriteHandler<TEntity> {

    void write(List<TEntity> entities) throws Exception;

}
