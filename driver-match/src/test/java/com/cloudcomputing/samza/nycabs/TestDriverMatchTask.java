package com.cloudcomputing.samza.nycabs;

import java.time.Duration;
import java.util.ListIterator;
import java.util.Map;
import java.util.HashMap;

import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.cloudcomputing.samza.nycabs.application.DriverMatchTaskApplication;



public class TestDriverMatchTask {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, String> confMap = new HashMap<>();
    InMemorySystemDescriptor isd;
    InMemoryInputDescriptor imdriverLocation, imevents;
    InMemoryOutputDescriptor outputMatchStream;


    @Before public void setup() {
        confMap.put("stores.driver-loc.factory", "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");
        confMap.put("stores.driver-loc.key.serde", "string");
        confMap.put("stores.driver-loc.msg.serde", "json");
        confMap.put("serializers.registry.json.class", "org.apache.samza.serializers.JsonSerdeFactory");
        confMap.put("serializers.registry.string.class", "org.apache.samza.serializers.StringSerdeFactory");

        isd = new InMemorySystemDescriptor("kafka");
        imdriverLocation = isd.getInputDescriptor("driver-locations", new NoOpSerde<>());
        imevents = isd.getInputDescriptor("events", new NoOpSerde<>());
        outputMatchStream = isd.getOutputDescriptor("match-stream", new NoOpSerde<>());
    }

    @Test
    public void testDriverMatchTask() throws Exception {
        TestRunner
                .of(new DriverMatchTaskApplication())
                .addInputStream(imevents, TestUtils.genStreamData("events"))
                .addInputStream(imdriverLocation, TestUtils.genStreamData("driver-locations"))
                .addOutputStream(outputMatchStream, 1)
                .addConfig(confMap)
                .addConfig("deploy.test", "true")
                .run(Duration.ofSeconds(5));

        Assert.assertEquals(5, TestRunner.consumeStream(outputMatchStream, Duration.ofSeconds(10)).get(0).size());

        ListIterator<Object> resultIter = TestRunner.consumeStream(outputMatchStream, Duration.ofSeconds(10)).get(0).listIterator();
        String genderTest = "{\"clientId\":3,\"driverId\":9002}";
        Assert.assertEquals(resultIter.next(), mapper.readTree(genderTest));

        String salaryTest = "{\"clientId\":4,\"driverId\":8000}";
        Assert.assertEquals(resultIter.next(), mapper.readTree(salaryTest));

        String ratingTest = "{\"clientId\":5,\"driverId\":8000}";
        Assert.assertEquals(resultIter.next(), mapper.readTree(ratingTest));

        String distanceTest = "{\"clientId\":6,\"driverId\":7001}";
        Assert.assertEquals(resultIter.next(), mapper.readTree(distanceTest));

        String rightBlockTest = "{\"clientId\":7,\"driverId\":3002}";
        Assert.assertEquals(resultIter.next(), mapper.readTree(rightBlockTest));
    }

    @Test
    public void testMultipleDriversSameBlock() throws Exception {
        TestRunner
                .of(new DriverMatchTaskApplication())
                .addInputStream(imevents, TestUtils.genStreamData("events"))
                .addInputStream(imdriverLocation, TestUtils.genStreamData("driver-locations"))
                .addOutputStream(outputMatchStream, 1)
                .addConfig(confMap)
                .addConfig("deploy.test", "true")
                .run(Duration.ofSeconds(5));

        ListIterator<Object> resultIter = TestRunner.consumeStream(outputMatchStream, Duration.ofSeconds(10)).get(0).listIterator();
        String multipleDriverTest = "{\"clientId\":3,\"driverId\":9002}";
        Assert.assertEquals(resultIter.next(), mapper.readTree(multipleDriverTest));
    }

    @Test
    public void testNoDriverAvailable() throws Exception {

        TestRunner
                .of(new DriverMatchTaskApplication())
                .addInputStream(imevents, TestUtils.genStreamData("nodrivers"))
                .addInputStream(imdriverLocation, TestUtils.genStreamData("driver-locations"))
                .addOutputStream(outputMatchStream, 1)
                .addConfig(confMap)
                .addConfig("deploy.test", "true")
                .run(Duration.ofSeconds(5));

        ListIterator<Object> resultIter = TestRunner.consumeStream(outputMatchStream, Duration.ofSeconds(10)).get(0).listIterator();
        Assert.assertNull(resultIter.next()); // No match should be made
    }


}