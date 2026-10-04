package com.broadcast.core.model.exception;
/**
 * A delivery failure that will never succeed no matter how many times
 * it's retried: invalid recipient, unsubscribed user, malformed address.
 */
public class PermanentDeliveryException extends DeliveryException{

    public PermanentDeliveryException(String message){
        super(message);
    }
    public PermanentDeliveryException(String message, Throwable cause){
        super(message,cause);
    }
}
