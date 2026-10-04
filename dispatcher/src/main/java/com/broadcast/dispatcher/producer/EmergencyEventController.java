package com.broadcast.dispatcher.producer;

import com.broadcast.core.model.BroadCastEvent;
import com.broadcast.dispatcher.config.Topics;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/broadcast")
public class EmergencyEventController {
    private final KafkaTemplate<String, BroadCastEvent>kafkaTemplate;

    public EmergencyEventController(KafkaTemplate<String, BroadCastEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public record BroadcastRequest(@NotBlank String region, @NotBlank String message, @NotEmpty List<String>userIds){

    }

    @PostMapping
    public ResponseEntity<Map<String,String>> triggerBroadcast(@Valid @RequestBody BroadcastRequest request){
        BroadCastEvent event = new BroadCastEvent(request.userIds(),request.message(),request.region);
        kafkaTemplate.send(Topics.RAW,event.getRegionId(),event);

        return ResponseEntity.accepted().body(Map.of(
                "broadcastId",event.getBroadcastId(),
                "status","accepted"
        ));
    }
}
