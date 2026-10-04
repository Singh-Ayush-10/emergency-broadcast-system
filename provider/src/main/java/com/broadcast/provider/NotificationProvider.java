package com.broadcast.provider;

import com.broadcast.core.model.exception.PermanentDeliveryException;
import com.broadcast.core.model.exception.TransientDeliveryException;

/**
 * The channel worker never talks to Twilio/SendGrid/FCM directly - only
 * to this interface. Swap the Spring bean (mock vs real) later; nothing
 * in the worker changes. A real provider is just a new implementation,
 * e.g. TwilioSmsProvider.
 */
public interface NotificationProvider {
    DeliveryResult send(String recipient, String message) throws TransientDeliveryException, PermanentDeliveryException;
}
