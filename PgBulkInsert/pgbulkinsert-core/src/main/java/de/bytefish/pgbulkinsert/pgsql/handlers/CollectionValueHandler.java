// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Collection;

public class CollectionValueHandler<TElementType, TCollectionType extends Collection<TElementType>> extends BaseValueHandler<TCollectionType> {

    private final int oid;
    private final IValueHandler<TElementType> valueHandler;

     public CollectionValueHandler(int oid, IValueHandler<TElementType> valueHandler) {
         this.oid = oid;
         this.valueHandler = valueHandler;
     }

    @Override
    protected void internalHandle(DataOutputStream buffer, TCollectionType value) throws Exception {

        ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
        DataOutputStream arrayOutput = new DataOutputStream(byteArrayOutput);

        arrayOutput.writeInt(1); // Dimensions, use 1 for one-dimensional arrays at the moment
        arrayOutput.writeInt(1); // The Array can contain Null Values
        arrayOutput.writeInt(oid); // Write the Values using the OID
        arrayOutput.writeInt(value.size()); // Write the number of elements
        arrayOutput.writeInt(1); // Ignore Lower Bound. Use PG Default for now

        // Now write the actual Collection elements using the inner handler:
        for (TElementType element : value) {
            valueHandler.handle(arrayOutput, element);
        }

        buffer.writeInt(byteArrayOutput.size());
        buffer.write(byteArrayOutput.toByteArray());
    }

    @Override
    public int getLength(TCollectionType value) {
        throw new UnsupportedOperationException();
    }
}
