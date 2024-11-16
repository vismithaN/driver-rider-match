package com.cloudcomputing.samza.nycabs;

import com.google.common.io.Resources;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestUtils {
    public static List<String> genStreamData(String channel) {
        List<String> streamDataRawStrings = null;
        if (channel.equals("events")) {
            streamDataRawStrings = readFile("events.txt");
        } else if (channel.equals("driver-locations")) {
            streamDataRawStrings = readFile("driverLocations.txt");
        }

        // Convert raw strings to JSON strings
        ObjectMapper objectMapper = new ObjectMapper();
        List<String> jsonStrings = streamDataRawStrings.stream()
                .map(raw -> {
                    try {
                        // Convert the raw string to JSON object and back to a JSON string
                        JsonParser parser = new JsonParser();
                        JsonElement jsonElement = parser.parse(raw);
                        JsonObject json = jsonElement.getAsJsonObject();
                        return json.toString();
                    } catch (Exception e) {
                        // Handle any parsing exceptions
                        System.err.println("Invalid JSON: " + raw);
                        e.printStackTrace();
                        return null; // Skip invalid JSON strings
                    }
                })
                .filter(json -> json != null) // Filter out invalid JSON strings
                .collect(Collectors.toList());

        System.out.println("Stream JSON Strings: " + jsonStrings);
        return jsonStrings;

//        return streamDataRawStrings.stream().map(s -> {
//            Map<String, Object> result;
//            ObjectMapper mapper = new ObjectMapper();
//            try {
//                result =  mapper.readValue(s, HashMap.class);
//                return result;
//            } catch (Exception e) {
//                System.out.println("Failed in parse " + s + ", skip into next line");
//            }
//            return null;
//        }).filter(x -> x != null).collect(Collectors.toList());
    }

    private static List<String> readFile(String path) {
        try {
            InputStream in = Resources.getResource(path).openStream();
            List<String> lines = new ArrayList<>();
            String line = null;
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            reader.close();
            return lines;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
