// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.configuration;

public class Configuration implements IConfiguration {

    private final int bufferSize;

    public Configuration() {
        this(65536);
    }

    public Configuration(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    @Override
    public int getBufferSize() {
        return bufferSize;
    }
}
