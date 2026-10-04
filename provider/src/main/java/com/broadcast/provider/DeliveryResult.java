package com.broadcast.provider;

public class DeliveryResult {
    private final String recipient;
    private final String providerMessageid;

    private DeliveryResult(String recipient, String providerMessageId){
        this.recipient = recipient;
        this.providerMessageid = providerMessageId;
    }
    public static DeliveryResult success(String recipient, String providerMessageId){
        return new DeliveryResult(recipient, providerMessageId);
    }
    public String getRecipient(){
        return recipient;
    }
    public String getProviderMessageId(){
        return providerMessageid;
    }

}
