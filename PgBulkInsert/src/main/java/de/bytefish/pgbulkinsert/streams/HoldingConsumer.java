package de.bytefish.pgbulkinsert.streams;

import java.util.function.Consumer;

public class HoldingConsumer implements Consumer {

    private Object value;

    @Override
    public void accept(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

}