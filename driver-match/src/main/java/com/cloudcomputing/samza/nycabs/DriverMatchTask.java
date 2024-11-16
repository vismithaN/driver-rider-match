package com.cloudcomputing.samza.nycabs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import com.google.gson.JsonObject;


/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask {

    /* Define per task state here. (kv stores etc)
       READ Samza API part in Primer to understand how to start
    */
    private KeyValueStore<String, JsonObject> driverLocStore;
    private static final double MAX_MONEY = 100.0;
    private static final double MAX_RATING = 5.0;


    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        // Initialize (maybe the kv stores?)
        driverLocStore = (KeyValueStore<String, JsonObject>) context.getTaskContext().getStore("driver-loc");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        /*
        All the messsages are partitioned by blockId, which means the messages
        sharing the same blockId will arrive at the same task, similar to the
        approach that MapReduce sends all the key value pairs with the same key
        into the same reducer.
        */
        String incomingStream = envelope.getSystemStreamPartition().getStream();
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(new ObjectMapper().writeValueAsString(envelope.getMessage()));
        JsonObject message = jsonElement.getAsJsonObject();

        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {

            handleDriverLocation(message);
        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {

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
        driverLocStore.put(driverId, message);
    }

    private void handleRideRequest(JsonObject message, MessageCollector collector) {
        int clientId = message.get("clientId").getAsInt();
        int blockId = message.get("blockId").getAsInt();
        String clientGenderPreference = message.get("gender_preference").getAsString();
        double clientLatitude = message.get("latitude").getAsDouble();
        double clientLongitude = message.get("longitude").getAsDouble();

        JsonObject bestMatchDriver = null;
        double highestMatchScore = -1;
        KeyValueIterator<String, JsonObject> drivers = driverLocStore.all();

        while (drivers.hasNext()) {
            Entry<String, JsonObject> entry = drivers.next();
            JsonObject driver = entry.getValue();
            if (driver.get("blockId").getAsInt() == blockId && "AVAILABLE".equals(driver.get("status").getAsString())) {
                double matchScore = calculateMatchScore(driver, clientLatitude, clientLongitude, clientGenderPreference);

                if (matchScore > highestMatchScore) {
                    highestMatchScore = matchScore;
                    bestMatchDriver = driver;
                }
            }
        }

        if (bestMatchDriver != null) {
            // Output match to match-stream
            int driverId = bestMatchDriver.get("driverId").getAsInt();
            JsonObject output = new JsonObject();
            output.addProperty("clientId", clientId);
            output.addProperty("driverId", driverId);
            collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, output.toString()));

            // Update driver status in the KV store
            bestMatchDriver.addProperty("status", "UNAVAILABLE");
            driverLocStore.put(String.valueOf(driverId), bestMatchDriver);
        }
    }

    private void handleRideComplete(JsonObject message) {
        String driverId = String.valueOf(message.get("driverId"));
        JsonObject driver = driverLocStore.get(driverId);
        if (driver != null) {
            // Update rating and mark driver as AVAILABLE
            double oldRating = driver.get("rating").getAsDouble();
            double userRating = message.get("user_rating").getAsDouble();
            driver.addProperty("rating", (oldRating + userRating) / 2);
            driver.addProperty("status", "AVAILABLE");

            // Update location if provided
            driver.addProperty("blockId", message.get("blockId").getAsInt());
            driver.addProperty("latitude", message.get("latitude").getAsDouble());
            driver.addProperty("longitude", message.get("longitude").getAsDouble());

            driverLocStore.put(driverId, driver);
        }
    }

    private void handleLeavingBlock(JsonObject message) {
        String driverId = String.valueOf(message.get("driverId"));
        JsonObject driver = driverLocStore.get(driverId);
        if (driver != null) {
            driver.addProperty("status", "UNAVAILABLE");
            driverLocStore.put(driverId, driver);
        }
    }

    private void handleEnteringBlock(JsonObject message) {
        String driverId = String.valueOf(message.get("driverId"));
        message.addProperty("status", "AVAILABLE");
        driverLocStore.put(driverId, message);
    }

    private double calculateMatchScore(JsonObject driver, double clientLatitude, double clientLongitude, String clientGenderPreference) {
        double driverLatitude = driver.get("latitude").getAsDouble();
        double driverLongitude = driver.get("longitude").getAsDouble();
        String driverGender = driver.get("gender").getAsString();
        double driverRating = driver.get("rating").getAsDouble();
        double driverSalary = driver.get("salary").getAsDouble();

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