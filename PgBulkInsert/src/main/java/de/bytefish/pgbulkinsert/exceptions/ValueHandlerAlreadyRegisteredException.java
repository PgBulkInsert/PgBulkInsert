// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.exceptions;

public class ValueHandlerAlreadyRegisteredException extends RuntimeException {

    public ValueHandlerAlreadyRegisteredException(String message) {
        super(message);
    }

    public ValueHandlerAlreadyRegisteredException() {
    }

    public ValueHandlerAlreadyRegisteredException(String message, Throwable cause) {
        super(message, cause);
    }

    public ValueHandlerAlreadyRegisteredException(Throwable cause) {
        super(cause);
    }

    public ValueHandlerAlreadyRegisteredException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
