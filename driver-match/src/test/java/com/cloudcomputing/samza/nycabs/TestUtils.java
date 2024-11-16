package com.cloudcomputing.samza.nycabs;

import com.google.common.io.Resources;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestUtils {
    public static List<JsonObject> genStreamData(String channel) {
        String streamDataRawStrings = null;
        if (channel.equals("events")) {
            streamDataRawStrings = "events.txt";
        } else if (channel.equals("driver-locations")) {
            streamDataRawStrings = "driverLocations.txt";
        }
        List<JsonObject> result = new ArrayList<>();
        // Convert raw strings to JSON strings
        try (BufferedReader br = new BufferedReader(new FileReader(streamDataRawStrings))) {
            String log;
            while ((log = br.readLine()) != null) {
                JsonParser parser = new JsonParser();
                JsonElement jsonElement = parser.parse(log);
                JsonObject json = jsonElement.getAsJsonObject();
                result.add(json);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        return result;

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
