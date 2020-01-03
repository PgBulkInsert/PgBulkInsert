package de.bytefish.pgbulkinsert.pgsql.handlers;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;

public class UUIDValueHandlerTest {

    @Test
    public void internalHandle() throws Exception {
        UUIDValueHandler handler = new UUIDValueHandler();
        for (int i = 0; i < 1000000; i++) {
            UUID uuid = UUID.randomUUID();

            ByteArrayOutputStream expected = new ByteArrayOutputStream();
            internalHandleOld(new DataOutputStream(expected), uuid);

            ByteArrayOutputStream actual = new ByteArrayOutputStream();
            handler.internalHandle(new DataOutputStream(actual), uuid);

            assertArrayEquals(expected.toByteArray(), actual.toByteArray());
        }
    }

    private static void internalHandleOld(DataOutputStream buffer, final UUID value) throws Exception {
        buffer.writeInt(16);

        ByteBuffer bb = toByteBuffer(value);

        buffer.writeInt(bb.getInt(0));
        buffer.writeShort(bb.getShort(4));
        buffer.writeShort(bb.getShort(6));

        buffer.write(Arrays.copyOfRange(bb.array(), 8, 16));
    }

    private static ByteBuffer toByteBuffer(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb;
    }
}
