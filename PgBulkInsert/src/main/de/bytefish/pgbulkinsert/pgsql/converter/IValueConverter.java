// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.converter;

public interface IValueConverter<TSource, TTarget> {

    TTarget convert(TSource source);

}
