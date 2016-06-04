// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.handlers;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Type;
import java.util.AbstractCollection;
import java.util.Collection;

public class CollectionValueHandler<S, T extends Collection<S>> extends BaseValueHandler<T> {

    private final Class<S> type;

    private final int oid;
    private final IValueHandler<S> valueHandler;

     public CollectionValueHandler(Class<S> type, int oid, IValueHandler<S> valueHandler) {
         this.type = type;
         this.oid = oid;
         this.valueHandler = valueHandler;
     }

    @Override
    public Type getTargetType() {
        return type;
    }

    @Override
    protected void internalHandle(DataOutputStream buffer, T value) throws Exception {

        ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
        DataOutputStream arrayOutput = new DataOutputStream(byteArrayOutput);

        arrayOutput.writeInt(1); // Dimensions, use 1 for one-dimensional arrays at the moment
        arrayOutput.writeInt(1); // The Array can contain Null Values
        arrayOutput.writeInt(oid); // Write the Values using the OID (TODO (Can we make this any easier for the user of the library?)
        arrayOutput.writeInt(value.size()); // Write the number of elements
        arrayOutput.writeInt(1); // Ignore Lower Bound. Use PG Default for now

        // Now write the actual Collection elements using the inner handler:
        for (S element : value) {
            valueHandler.handle(arrayOutput, element);
        }

        buffer.writeInt(byteArrayOutput.size());
        buffer.write(byteArrayOutput.toByteArray());
    }
}
