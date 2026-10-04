package com.broadcast.tracking;

import com.broadcast.core.model.DeliveryStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/broadcast")
public class DeliveryStatusController {
    private final DeliveryTrackerService trackerService;

    public DeliveryStatusController(DeliveryTrackerService trackerService) {
        this.trackerService = trackerService;
    }
    public record StatusResponse(String broadcastId, Map<DeliveryStatus,Long> summary, List<DeliveryLog>deliveries){

    }
    @GetMapping("/{broadcastId}/status")
    public StatusResponse getStatus(@PathVariable String broadcastId){
        return new StatusResponse(
                broadcastId,
                trackerService.summarize(broadcastId),
                trackerService.findAll(broadcastId)
        );
    }
}
