import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LoadTest {

    public static void main(String[] args) throws Exception {
        int userCount = 1000;

        StringBuilder userIdsJson = new StringBuilder("[");
        for (int i = 1; i <= userCount; i++) {
            if (i > 1) userIdsJson.append(",");
            userIdsJson.append("\"user-").append(i).append("\"");
        }
        userIdsJson.append("]");

        String requestBody = """
                {
                  "region": "Pune",
                  "message": "load test - %d users",
                  "userIds": %s
                }
                """.formatted(userCount, userIdsJson);

        HttpClient client = HttpClient.newHttpClient();

        System.out.println("Sending broadcast with " + userCount + " users...");
        Instant start = Instant.now();

        HttpRequest postRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8080/broadcast"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> postResponse = client.send(postRequest, HttpResponse.BodyHandlers.ofString());
        System.out.println("Accepted in " + Duration.between(start, Instant.now()).toMillis() + " ms");

        String broadcastId = extractBroadcastId(postResponse.body());
        System.out.println("broadcastId=" + broadcastId);

        System.out.println("Polling /status until nothing is left PENDING or RETRYING...");
        Thread.sleep(3000);

        int inFlight;
        Instant deadline = start.plus(Duration.ofMinutes(10));

        do {
            HttpRequest statusRequest = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:8090/broadcast/" + broadcastId + "/status"))
                    .GET().build();

            HttpResponse<String> statusResponse = client.send(statusRequest, HttpResponse.BodyHandlers.ofString());
            String body = statusResponse.body();

            int totalTracked = sumSummaryCounts(body);
            inFlight = inFlightCount(body);
            long elapsed = Duration.between(start, Instant.now()).toSeconds();

            System.out.printf("  [%ds] tracked=%d  in-flight=%s%n",
                    elapsed, totalTracked, inFlight == -1 ? "not ready yet" : String.valueOf(inFlight));

            if (inFlight == 0) break;
            Thread.sleep(2000);

        } while (Instant.now().isBefore(deadline));

        Duration totalElapsed = Duration.between(start, Instant.now());
        System.out.println("\nDone. Total elapsed: " + totalElapsed.toSeconds() + " seconds");
        System.out.printf("Throughput: %.2f users/sec%n", userCount / (double) totalElapsed.toSeconds());
    }

    private static String extractBroadcastId(String json) {
        Matcher m = Pattern.compile("\"broadcastId\"\\s*:\\s*\"([^\"]+)\"").matcher(json);
        return m.find() ? m.group(1) : "UNKNOWN";
    }

    private static int sumSummaryCounts(String json) {
        String section = extractSummarySection(json);
        if (section == null) return 0;
        Matcher m = Pattern.compile(":\\s*(\\d+)").matcher(section);
        int sum = 0;
        while (m.find()) sum += Integer.parseInt(m.group(1));
        return sum;
    }

    private static int inFlightCount(String json) {
        String section = extractSummarySection(json);
        if (section == null) return -1;
        int inFlight = 0;
        Matcher p = Pattern.compile("\"PENDING\"\\s*:\\s*(\\d+)").matcher(section);
        if (p.find()) inFlight += Integer.parseInt(p.group(1));
        Matcher r = Pattern.compile("\"RETRYING\"\\s*:\\s*(\\d+)").matcher(section);
        if (r.find()) inFlight += Integer.parseInt(r.group(1));
        return inFlight;
    }

    private static String extractSummarySection(String json) {
        int s = json.indexOf("\"summary\"");
        int d = json.indexOf("\"deliveries\"");
        if (s == -1 || d == -1) return null;
        return json.substring(s, d);
    }
}