// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.handlers;

import java.lang.reflect.Type;

public interface IValueHandlerProvider {

    <TTargetType> IValueHandler<TTargetType> resolve(Type targetType);

}
