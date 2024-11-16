package com.cloudcomputing.samza.nycabs;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;


import java.util.Map;

public class DriverMatchTask implements StreamTask, InitableTask {

    private KeyValueStore<String, Map<String, Object>> driverLocStore;
    private final static double MAX_MONEY = 100.0;
    private final static double MAX_RATING = 5.0;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) {
        // Initialize the KeyValueStore (assuming driverLocStore is defined in the config)
        driverLocStore = (KeyValueStore<String, Map<String, Object>>) context.getTaskContext().getStore("driver-loc");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        String incomingStream = envelope.getSystemStreamPartition().getStream();
        Gson gson = new Gson();
        JsonObject message = gson.toJsonTree( envelope.getMessage()).getAsJsonObject();

        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
            // Handle Driver Location messages
            handleDriverLocation(message);
        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
            // Handle Event messages
            String eventType = message.get("type").getAsString();
            switch (eventType) {
                case "RIDE_REQUEST":
                    handleRideRequest(message, collector);
                    break;
                case "RIDE_COMPLETE":
                    handleRideComplete(message);
                    break;
                case "LEAVING_BLOCK":
                    handleLeavingBlock(message);
                    break;
                case "ENTERING_BLOCK":
                    handleEnteringBlock(message);
                    break;
                default:
                    throw new IllegalStateException("Unexpected event type: " + eventType);
            }
        }
    }

    private void handleDriverLocation(JsonObject message) {
        // Store driver location and update availability
        String driverId = String.valueOf(message.get("driverId").getAsInt());
        Gson gson = new Gson();
        Map<String,Object> value = gson.fromJson(message, Map.class);
        driverLocStore.put(driverId, value);
    }

    private void handleRideRequest(JsonObject message, MessageCollector collector) {
        int clientId = message.get("clientId").getAsInt();
        int blockId = message.get("blockId").getAsInt();
        String clientGenderPreference = message.has("gender_preference")
                ? message.get("gender_preference").getAsString()
                : "N";
        double clientLatitude = message.get("latitude").getAsDouble();
        double clientLongitude = message.get("longitude").getAsDouble();

        Map<String, Object> bestMatchDriver = null;
        double highestMatchScore = -1;
        KeyValueIterator<String, Map<String, Object>> drivers = driverLocStore.all();

        while (drivers.hasNext()) {
            Map<String, Object> driver = drivers.next().getValue();
            if (driver.get("blockId").equals(blockId) && "AVAILABLE".equals(driver.get("status"))) {
                double matchScore = calculateMatchScore(driver, clientLatitude, clientLongitude, clientGenderPreference);

                if (matchScore > highestMatchScore) {
                    highestMatchScore = matchScore;
                    bestMatchDriver = driver;
                }
            }
        }

        if (bestMatchDriver != null) {
            // Output match to match-stream
            int driverId = Integer.parseInt(bestMatchDriver.get("driverId"));
            JsonObject output = new JsonObject();
            output.addProperty("clientId", clientId);
            output.addProperty("driverId", driverId);
            collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, output.toString()));

            // Update driver status in the KV store
            bestMatchDriver.put("status", "UNAVAILABLE");
            driverLocStore.put(String.valueOf(driverId), bestMatchDriver);
        }
    }

    private void handleRideComplete(JsonObject message) {
        String driverId = String.valueOf(message.get("driverId").getAsInt());
        Map<String, Object> driver = driverLocStore.get(driverId);
        if (driver != null) {
            // Update rating and mark driver as AVAILABLE
            double oldRating = (double) driver.get("rating");
            double userRating = message.get("user_rating").getAsDouble();
            driver.put("rating", (oldRating + userRating) / 2);
            driver.put("status", "AVAILABLE");

            // Update location if provided
            driver.put("blockId", message.get("blockId").getAsInt());
            driver.put("latitude", message.get("latitude").getAsDouble());
            driver.put("longitude", message.get("longitude").getAsDouble());

            driverLocStore.put(driverId, driver);
        }
    }

    private void handleLeavingBlock(JsonObject message) {
        String driverId = String.valueOf(message.get("driverId").getAsInt());
        Map<String, Object> driver = driverLocStore.get(driverId);
        if (driver != null) {
            driver.put("status", "UNAVAILABLE");
            driverLocStore.put(driverId, driver);
        }
    }

    private void handleEnteringBlock(JsonObject message) {
        String driverId = String.valueOf(message.get("driverId").getAsInt());
        Gson gson = new Gson();
        Map<String, Object> driverInfo = gson.fromJson(message,Map.class);
        driverInfo.put("status", "AVAILABLE");
        driverLocStore.put(driverId, driverInfo);
    }

    private double calculateMatchScore(Map<String, Object> driver, double clientLatitude, double clientLongitude, String clientGenderPreference) {
        double driverLatitude = (double) driver.get("latitude");
        double driverLongitude = (double) driver.get("longitude");
        String driverGender = (String) driver.get("gender");
        double driverRating = (double) driver.get("rating");
        double driverSalary = (double) driver.get("salary");

        // Calculate distance score
        double distance = Math.sqrt(Math.pow(clientLatitude - driverLatitude, 2) + Math.pow(clientLongitude - driverLongitude, 2));
        double distanceScore = Math.exp(-distance);

        // Calculate gender score
        double genderScore = clientGenderPreference.equals("N") || clientGenderPreference.equals(driverGender) ? 1.0 : 0.0;

        // Calculate rating and salary scores
        double ratingScore = driverRating / MAX_RATING;
        double salaryScore = 1 - (driverSalary / MAX_MONEY);

        // Calculate weighted match score
        return distanceScore * 0.4 + genderScore * 0.1 + ratingScore * 0.3 + salaryScore * 0.2;
    }
}