// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.constants.DataType;
import de.bytefish.pgbulkinsert.pgsql.model.network.MacAddress;

import java.io.DataOutputStream;
import java.net.Inet4Address;

public class MacAddressValueHandler extends BaseValueHandler<MacAddress> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final MacAddress value) throws Exception {
        buffer.writeInt(6);
        buffer.write(value.getAddressBytes());
    }

    @Override
    public DataType getDataType() {
        return DataType.MacAddress;
    }
}
