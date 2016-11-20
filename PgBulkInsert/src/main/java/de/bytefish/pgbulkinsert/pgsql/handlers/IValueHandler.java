// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.constants.DataType;

import java.io.DataOutputStream;

public interface IValueHandler<TTargetType> extends ValueHandler {

    void handle(DataOutputStream buffer, final TTargetType value);

    DataType getDataType();

}
