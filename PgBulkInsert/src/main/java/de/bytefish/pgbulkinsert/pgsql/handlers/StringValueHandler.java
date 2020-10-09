// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import java.io.DataOutputStream;

import de.bytefish.pgbulkinsert.util.StringUtils;

public class StringValueHandler extends BaseValueHandler<String> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final String value) throws Exception {
        byte[] utf8Bytes = StringUtils.getUtf8Bytes(value);

        buffer.writeInt(utf8Bytes.length);
        buffer.write(utf8Bytes);
    }

    @Override
    public int getLength(String value) {
        byte[] utf8Bytes = StringUtils.getUtf8Bytes(value);

        return utf8Bytes.length;
    }
}
