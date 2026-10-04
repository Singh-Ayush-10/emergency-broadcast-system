package com.broadcast;

import com.broadcast.provider.MockNotificationProvider;
import com.broadcast.core.model.exception.PermanentDeliveryException;
import com.broadcast.core.model.exception.TransientDeliveryException;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
        System.out.printf("Hello and welcome!");

        MockNotificationProvider provider = new MockNotificationProvider("sms",0.3,0.3,50,150);
        int delivered = 0;
        int transient_ = 0;
        int permanent = 0;

        for(int i=0;i<=20;i++){
            try{
                var result  = provider.send("+91-9999999999", "test message");
                delivered++;
                System.out.println(i + ": DELIVERED -> " + result.getProviderMessageId());
            }catch (TransientDeliveryException e) {
                transient_++;
                System.out.println(i + ": TRANSIENT -> " + e.getMessage());
            } catch (PermanentDeliveryException e) {
                permanent++;
                System.out.println(i + ": PERMANENT -> " + e.getMessage());
            }
        }
        System.out.println("\n--- Summary over 20 calls ---");
        System.out.println("Delivered: " + delivered + " (expected ~40%)");
        System.out.println("Transient: " + transient_ + " (expected ~30%)");
        System.out.println("Permanent: " + permanent + " (expected ~30%)");
    }
}