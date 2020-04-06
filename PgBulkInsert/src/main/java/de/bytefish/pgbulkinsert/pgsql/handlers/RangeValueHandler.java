// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.model.range.Range;

import java.io.DataOutputStream;

public class RangeValueHandler<TElementType> extends BaseValueHandler<Range<TElementType>> {

    private final IValueHandler<TElementType> valueHandler;

    public RangeValueHandler(IValueHandler<TElementType> valueHandler) {
        this.valueHandler = valueHandler;
    }

    @Override
    protected void internalHandle(DataOutputStream buffer, Range<TElementType> value) throws Exception {
        buffer.writeInt(getLength(value));
        buffer.writeByte((byte)value.getFlags());

        if (value.isEmpty()) {
            return;
        }

        if(!value.isUpperBoundInfinite()) {
            valueHandler.handle(buffer, value.getLowerBound());
        }

        if(!value.isUpperBoundInfinite()) {
            valueHandler.handle(buffer, value.getUpperBound());
        }
    }

    @Override
    public int getLength(Range<TElementType> value) {
        int totalLen = 1;

        if (!value.isEmpty())
        {
            if (!value.isLowerBoundInfinite())
                totalLen += 4 + valueHandler.getLength(value.getLowerBound());

            if (!value.isUpperBoundInfinite())
                totalLen += 4 + valueHandler.getLength(value.getUpperBound());
        }

        return totalLen;
    }
}

