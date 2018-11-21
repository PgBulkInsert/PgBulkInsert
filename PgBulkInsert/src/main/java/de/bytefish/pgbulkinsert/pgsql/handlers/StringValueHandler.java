// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import java.io.DataOutputStream;
import java.nio.charset.Charset;

public class StringValueHandler extends BaseValueHandler<String> {
	
	private Charset utf8Charset = Charset.forName("UTF-8");

    @Override
    protected void internalHandle(DataOutputStream buffer, final String value) throws Exception {
        byte[] utf8Bytes = value.getBytes(utf8Charset);

        buffer.writeInt(utf8Bytes.length);
        buffer.write(utf8Bytes);
    }
}
