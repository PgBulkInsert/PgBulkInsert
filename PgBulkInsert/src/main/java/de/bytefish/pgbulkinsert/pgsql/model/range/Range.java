// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.model.range;

import java.util.Objects;

// https://github.com/npgsql/npgsql/blob/d4132d0d546594629bcef658bcb1418b4a8624cc/src/Npgsql/NpgsqlTypes/NpgsqlRange.cs
public class Range<TElementType> {

    private int flags;

    private TElementType lowerBound;

    private TElementType upperBound;

    public Range(TElementType lowerBound, TElementType upperBound) {
        this(lowerBound, true, false, upperBound, true, false);
    }

    public Range(TElementType lowerBound, boolean lowerBoundIsInclusive, TElementType upperBound, boolean upperBoundIsInclusive) {
        this(lowerBound, lowerBoundIsInclusive, false, upperBound, upperBoundIsInclusive, false);
    }

    public Range(TElementType lowerBound, boolean lowerBoundIsInclusive, boolean lowerBoundInfinite,
                 TElementType upperBound, boolean upperBoundIsInclusive, boolean upperBoundInfinite) {
        this(lowerBound, upperBound, evaluateBoundaryFlags(lowerBoundIsInclusive, upperBoundIsInclusive, lowerBoundInfinite, upperBoundInfinite));
    }


    private Range(TElementType lowerBound, TElementType upperBound, int flags) {
        this.lowerBound = (flags & RangeFlags.LowerBoundInfinite) != 0 ? null : lowerBound;
        this.upperBound = (flags & RangeFlags.UpperBoundInfinite) != 0 ? null : upperBound;
        this.flags = flags;

        // TODO Check this!
        if (lowerBound == null) {
            this.flags |= RangeFlags.LowerBoundInfinite;
        }

        if (upperBound == null) {
            this.flags |= RangeFlags.UpperBoundInfinite;
        }

        if (isEmptyRange(lowerBound, upperBound, flags)) {
            this.lowerBound = null;
            this.upperBound = null;
            this.flags = RangeFlags.Empty;
        }
    }

    private boolean isEmptyRange(TElementType lowerBound, TElementType upperBound, int flags) {
        // ---------------------------------------------------------------------------------
        // We only want to check for those conditions that are unambiguously erroneous:
        //   1. The bounds must not be default values (including null).
        //   2. The bounds must be definite (non-infinite).
        //   3. The bounds must be inclusive.
        //   4. The bounds must be considered equal.
        //
        // See:
        //  - https://github.com/npgsql/npgsql/pull/1939
        //  - https://github.com/npgsql/npgsql/issues/1943
        // ---------------------------------------------------------------------------------

        if ((flags & RangeFlags.Empty) == RangeFlags.Empty)
            return true;

        if ((flags & RangeFlags.Infinite) == RangeFlags.Infinite)
            return false;

        if ((flags & RangeFlags.Inclusive) == RangeFlags.Inclusive)
            return false;

        return Objects.equals(lowerBound, upperBound);
    }


    private static int evaluateBoundaryFlags(boolean lowerBoundIsInclusive, boolean upperBoundIsInclusive, boolean lowerBoundInfinite, boolean upperBoundInfinite) {

        int result = RangeFlags.None;

        // This is the only place flags are calculated.
        if (lowerBoundIsInclusive)
            result |= RangeFlags.LowerBoundInclusive;
        if (upperBoundIsInclusive)
            result |= RangeFlags.UpperBoundInclusive;
        if (lowerBoundInfinite)
            result |= RangeFlags.LowerBoundInfinite;
        if (upperBoundInfinite)
            result |= RangeFlags.UpperBoundInfinite;

        // PostgreSQL automatically converts inclusive-infinities.
        // See: https://www.postgresql.org/docs/current/static/rangetypes.html#RANGETYPES-INFINITE
        if ((result & RangeFlags.LowerInclusiveInfinite) == RangeFlags.LowerInclusiveInfinite) {
            result &= ~RangeFlags.LowerBoundInclusive;
        }

        if ((result & RangeFlags.UpperInclusiveInfinite) == RangeFlags.UpperInclusiveInfinite) {
            result &= ~RangeFlags.UpperBoundInclusive;
        }

        return result;
    }

    public int getFlags() {
        return flags;
    }

    public boolean isEmpty() {
        return (flags & RangeFlags.Empty) != 0;
    }

    public boolean isLowerBoundInfinite() {
        return (flags & RangeFlags.LowerBoundInfinite) != 0;
    }

    public boolean isUpperBoundInfinite() {
        return (flags & RangeFlags.UpperBoundInfinite) != 0;
    }

    public TElementType getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(TElementType lowerBound) {
        this.lowerBound = lowerBound;
    }

    public TElementType getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(TElementType upperBound) {
        this.upperBound = upperBound;
    }
}
