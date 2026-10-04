package com.broadcast.core.model.exception;

public class TransientDeliveryException extends DeliveryException{

    public TransientDeliveryException(String message){
        super(message);
    }
    public TransientDeliveryException(String message, Throwable cause){
        super(message, cause);
    }


}
