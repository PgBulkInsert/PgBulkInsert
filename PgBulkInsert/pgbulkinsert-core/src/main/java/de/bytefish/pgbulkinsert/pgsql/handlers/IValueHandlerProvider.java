// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.constants.DataType;

public interface IValueHandlerProvider {

    <TTargetType> IValueHandler<TTargetType> resolve(DataType targetType);

}
