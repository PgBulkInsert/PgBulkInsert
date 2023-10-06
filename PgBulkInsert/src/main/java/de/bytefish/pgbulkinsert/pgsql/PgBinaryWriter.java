// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql;

import de.bytefish.pgbulkinsert.exceptions.BinaryWriteFailedException;
import de.bytefish.pgbulkinsert.pgsql.handlers.IValueHandler;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class PgBinaryWriter implements AutoCloseable {

    private final transient DataOutputStream buffer;

    public PgBinaryWriter(final OutputStream out) {
        this(out, 65536);
    }

    public PgBinaryWriter(final OutputStream out, final int bufferSize) {
		buffer = new DataOutputStream(new BufferedOutputStream(out, bufferSize));
		writeHeader();
    }

    public void startRow(int numColumns) {
        try {
            buffer.writeShort(numColumns);
		} catch (IOException e) {
			Throwable t = e.getCause();
			if (null != t) {
				throw new BinaryWriteFailedException(t);
			} else {
				throw new BinaryWriteFailedException(e);
			}
		}
    }

    public <TTargetType> void write(final IValueHandler<TTargetType> handler, final TTargetType value) {
        handler.handle(buffer, value);
    }

    /**
     * Writes primitive boolean to the output stream
     *
     * @param value value to write
     *
     */
	public void writeBoolean(boolean value) {
		try {
			buffer.writeInt(1);
			if (value) {
				buffer.writeByte(1);
			} else {
				buffer.writeByte(0);
			}
		} catch (IOException e) {
			Throwable t = e.getCause();
			if (null != t) {
				throw new BinaryWriteFailedException(t);
			} else {
				throw new BinaryWriteFailedException(e);
			}
		}
	}


    /**
     * Writes primitive byte to the output stream
     *
     * @param value value to write
     *
     */
	public void writeByte(int value) {
		try {
			buffer.writeInt(1);
			buffer.writeByte(value);
		} catch (IOException e) {
			Throwable t = e.getCause();
			if (null != t) {
				throw new BinaryWriteFailedException(t);
			} else {
				throw new BinaryWriteFailedException(e);
			}
		}
	}

    /**
     * Writes primitive short to the output stream
     *
     * @param value value to write
     *
     */
	public void writeShort(int value) {
		try {
			buffer.writeInt(2);
			buffer.writeShort(value);
		} catch (IOException e) {
			Throwable t = e.getCause();
			if (null != t) {
				throw new BinaryWriteFailedException(t);
			} else {
				throw new BinaryWriteFailedException(e);
			}
		}
	}

    /**
     * Writes primitive integer to the output stream
     *
     * @param value value to write
     *
     */
	public void writeInt(int value) {
		try {
			buffer.writeInt(4);
			buffer.writeInt(value);
		} catch (IOException e) {
			Throwable t = e.getCause();
			if (null != t) {
				throw new BinaryWriteFailedException(t);
			} else {
				throw new BinaryWriteFailedException(e);
			}
		}
	}

    /**
     * Writes primitive long to the output stream
     *
     * @param value value to write
     *
     */
	public void writeLong(long value) {
		try {
			buffer.writeInt(8);
			buffer.writeLong(value);
		} catch (IOException e) {
			Throwable t = e.getCause();
			if (null != t) {
				throw new BinaryWriteFailedException(t);
			} else {
				throw new BinaryWriteFailedException(e);
			}
		}
	}

    /**
     * Writes primitive float to the output stream
     *
     * @param value value to write
     *
     */
	public void writeFloat(float value) {
		try {
	        buffer.writeInt(4);
	        buffer.writeFloat(value);
		} catch (IOException e) {
			Throwable t = e.getCause();
			if (null != t) {
				throw new BinaryWriteFailedException(t);
			} else {
				throw new BinaryWriteFailedException(e);
			}
		}
	}

    /**
     * Writes primitive double to the output stream
     *
     * @param value value to write
     *
     */
	public void writeDouble(double value) {
		try {
			buffer.writeInt(8);
			buffer.writeDouble(value);
		} catch (IOException e) {
			Throwable t = e.getCause();
			if (null != t) {
				throw new BinaryWriteFailedException(t);
			} else {
				throw new BinaryWriteFailedException(e);
			}
		}
	}

	/**
	 * Writes a Null Value.
	 */
	public void writeNull() {
		try {
			buffer.writeInt(-1);
		} catch (IOException e) {
			Throwable t = e.getCause();
			if (null != t) {
				throw new BinaryWriteFailedException(t);
			} else {
				throw new BinaryWriteFailedException(e);
			}
		}
	}

    @Override
    public void close() {
        try {
            buffer.writeShort(-1);

            buffer.flush();
            buffer.close();
		} catch (Exception e) {
			Throwable t = e.getCause();
			if (null != t) {
				throw new BinaryWriteFailedException(t);
			} else {
				throw new BinaryWriteFailedException(e);
			}
		}
    }

    private void writeHeader() {
        try {

            // 11 bytes required header
            buffer.writeBytes("PGCOPY\n\377\r\n\0");
            // 32 bit integer indicating no OID
            buffer.writeInt(0);
            // 32 bit header extension area length
            buffer.writeInt(0);

		} catch (IOException e) {
			Throwable t = e.getCause();
			if (null != t) {
				throw new BinaryWriteFailedException(t);
			} else {
				throw new BinaryWriteFailedException(e);
			}
		}
    }
}
