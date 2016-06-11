// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.util;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class JavaUtils {

    public static <TKey, TValue> Map<TKey, TValue> initializeMap(AbstractMap.SimpleEntry<TKey, TValue>... entries) {
        return Arrays.asList(entries)
                .stream()
                .collect(Collectors.toMap((se) -> se.getKey(), (se) -> se.getValue()));
    }

}
