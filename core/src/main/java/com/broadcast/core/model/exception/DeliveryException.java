package com.broadcast.core.model.exception;

/**
 * Base type for a failed delivery attempt. Never thrown directly - use
 * TransientDeliveryException or PermanentDeliveryException so the
 * channel worker can branch its retry/DLT behaviour on the type.
 *
 * Checked deliberately: NotificationProvider.send() declares both
 * subtypes in its throws clause, which forces every provider
 * implementation to explicitly classify each failure path at compile
 * time - you can't accidentally throw an unclassified failure.
 */
public abstract class DeliveryException extends Exception {
    protected DeliveryException(String message){
        super(message);
    }
    protected DeliveryException(String message, Throwable cause){
        super(message,cause);
    }
}
