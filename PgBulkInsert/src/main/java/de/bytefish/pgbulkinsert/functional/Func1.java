// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.functional;

@FunctionalInterface
public interface Func1<S> {
    S invoke() throws Exception;
}
