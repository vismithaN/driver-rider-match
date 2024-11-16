package com.cloudcomputing.samza.nycabs;

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class DriverMatchTask implements StreamTask, InitableTask {

    private KeyValueStore<String, Map<String, Object>> driverLocStore;
    private final double MAX_MONEY = 100.0;
    private final double MAX_RATING = 5.0;
    private final  ObjectMapper mapper = new ObjectMapper();

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) {
        // Initialize the KeyValueStore (assuming driverLocStore is defined in the config)
        driverLocStore = (KeyValueStore<String, Map<String, Object>>) context.getTaskContext().getStore("driver-loc");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String incomingStream = envelope.getSystemStreamPartition().getStream();
        Map<String,Object> message = (Map<String, Object>)envelope.getMessage();

        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
            // Handle Driver Location messages
            handleDriverLocation(message); //no
        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
            // Handle Event messages
            String eventType = message.get("type").toString();
            switch (eventType) {
                case "RIDE_REQUEST": //no
                    handleRideRequest(message, collector);
                    break;
                case "RIDE_COMPLETE": //yes
                    handleRideComplete(message);
                    break;
                case "LEAVING_BLOCK": //no
                    handleLeavingBlock(message);
                    break;
                case "ENTERING_BLOCK": //yes
                    handleEnteringBlock(message);
                    break;
                default:
                    throw new IllegalStateException("Unexpected event type: " + eventType);
            }
        }
    }

    private void handleDriverLocation(Map<String,Object> message) {
        // Store driver location and update availability
        String driverId = String.valueOf(message.get("driverId"));
        driverLocStore.put(driverId, message);
    }

    private void handleRideRequest(Map<String,Object> message, MessageCollector collector) throws Exception {
        int clientId = Integer.parseInt(message.get("clientId").toString());
        int clientBlockId = Integer.parseInt(message.get("blockId").toString());
        String clientGenderPreference = message.get("gender_preference") == null
                ? "N" : message.get("gender_preference").toString();
        double clientLatitude = Double.parseDouble(message.get("latitude").toString());
        double clientLongitude = Double.parseDouble(message.get("longitude").toString());

        Map<String, Object> bestMatchDriver = null;
        double highestMatchScore = Double.MIN_VALUE;
        KeyValueIterator<String,Map<String,Object>> iterator = driverLocStore.all();

        while (iterator.hasNext()) {
            Map<String, Object> driver = iterator.next().getValue();
            if (Integer.parseInt(driver.get("blockId").toString()) == clientBlockId &&
                    "AVAILABLE".equals(driver.get("status").toString())) {
                double matchScore = calculateMatchScore(driver, clientLatitude, clientLongitude, clientGenderPreference);

                if (matchScore > highestMatchScore) {
                    highestMatchScore = matchScore;
                    bestMatchDriver = driver;
                }
            }
        }

        if (bestMatchDriver != null) {
            // Output match to match-stream
            int driverId = Integer.parseInt(bestMatchDriver.get("driverId").toString());
            Map<String,Object> output = new HashMap<>();
            output.put("clientId", clientId);
            output.put("driverId", driverId);

            mapper.readTree(mapper.writeValueAsString(output));
            collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, mapper.readTree(mapper.writeValueAsString(output))));

            // Update driver status in the KV store
            driverLocStore.delete(String.valueOf(driverId));
        }
    }

    private void handleRideComplete(Map<String,Object> message) {
        String driverId = String.valueOf(message.get("driverId"));
        Map<String, Object> driver = driverLocStore.get(driverId);

        // Update rating and mark driver as AVAILABLE
        if (driver != null) {
            double oldRating = Double.parseDouble(message.get("rating").toString());
            double userRating = Double.parseDouble(message.get("user_rating").toString());
            driver.put("rating", (oldRating + userRating) / 2);
            driver.put("status", "AVAILABLE");
            driver.put("gender", message.get("gender").toString());
            driver.put("salary", Integer.parseInt(message.get("salary").toString()));

            // Update location if provided
            driver.put("blockId", Integer.parseInt(message.get("blockId").toString()));
            driver.put("latitude", Double.parseDouble(message.get("latitude").toString()));
            driver.put("longitude", Double.parseDouble(message.get("longitude").toString()));
            driverLocStore.put(driverId, driver);
        } else {
            Map<String, Object> driverNew = new HashMap<>();
            driverNew.put("driverId", driverId);
            driverNew.put("blockId", Integer.parseInt(message.get("blockId").toString()));
            driverNew.put("latitude", Double.parseDouble(message.get("latitude").toString()));
            driverNew.put("longitude", Double.parseDouble(message.get("longitude").toString()));
            driverNew.put("rating", Double.parseDouble(message.get("rating").toString()));
            driverNew.put("status", "AVAILABLE");
            driverNew.put("gender", message.get("gender").toString());
            driverNew.put("salary", Integer.parseInt(message.get("salary").toString()));
            driverLocStore.put(driverId, driverNew);
        }

    }

    private void handleLeavingBlock(Map<String,Object> message) {
        String driverId = String.valueOf(message.get("driverId"));
        driverLocStore.delete(driverId);
    }

    private void handleEnteringBlock(Map<String,Object> message) {
        String driverId = String.valueOf(message.get("driverId"));
        Map<String, Object> driverInfo = message;
        driverInfo.put("status", "AVAILABLE");
        driverLocStore.put(driverId, driverInfo);
    }

    private double calculateMatchScore(Map<String, Object> driver, double clientLatitude,
                                       double clientLongitude, String clientGenderPreference) {
        double driverLatitude = (double) driver.get("latitude");
        double driverLongitude = (double) driver.get("longitude");
        String driverGender = (String) driver.get("gender");
        double driverRating = (double) (driver.get("rating") == null ? 0.0 : driver.get("rating"));
        int driverSalary =  (int) (driver.get("salary") == null ? 0 : driver.get("salary"));


        // Calculate distance score
        double distance = Math.sqrt(Math.pow(clientLatitude - driverLatitude, 2) +
                Math.pow(clientLongitude - driverLongitude, 2));
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
