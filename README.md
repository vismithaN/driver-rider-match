# Driver-Rider Matching System

A real-time stream processing application for matching taxi drivers with riders based on GPS location data. Built using Apache Samza and Apache Kafka for distributed stream processing.

## Overview

This application processes two Kafka streams:
- **Driver Location Stream**: Real-time GPS coordinates of available drivers
- **Event Stream**: Rider cab requests with pickup locations

The system matches riders to the nearest available drivers in real-time and outputs the matches to a result stream.

## Architecture

### System Architecture Flow

```
┌─────────────────┐         ┌──────────────────┐
│  Driver GPS     │         │  Rider Requests  │
│  Location Data  │         │     (Events)     │
└────────┬────────┘         └────────┬─────────┘
         │                           │
         └──────────┬────────────────┘
                    │
                    ▼
         ┌─────────────────────┐
         │   Apache Kafka      │
         │   Message Broker    │
         └──────────┬──────────┘
                    │
         ┌──────────┴──────────┐
         │                     │
         ▼                     ▼
┌────────────────┐    ┌────────────────┐
│driver-locations│    │     events     │
│     topic      │    │     topic      │
└────────┬───────┘    └────────┬───────┘
         │                     │
         └──────────┬──────────┘
                    │
                    ▼
         ┌─────────────────────┐
         │   Apache Samza      │
         │  DriverMatchTask    │
         │                     │
         │  - Process streams  │
         │  - Match algorithm  │
         │  - State storage    │
         └──────────┬──────────┘
                    │
                    ▼
         ┌─────────────────────┐
         │    match-stream     │
         │   (Output Topic)    │
         └─────────────────────┘
```

### Data Flow

1. **Input Processing**: 
   - Driver locations and rider events are partitioned by `blockId`
   - Messages with the same `blockId` are processed by the same Samza task
   - This ensures locality-based matching efficiency

2. **State Management**:
   - RocksDB key-value store maintains driver availability and locations
   - Stateful processing enables tracking of driver positions over time

3. **Matching Logic**:
   - Processes driver location updates to maintain available driver pool
   - Matches incoming rider requests with nearest available drivers
   - Outputs matched rider-driver pairs to the output stream

## Technology Stack

- **Apache Samza 1.2.0**: Stream processing framework
- **Apache Kafka 0.10.1.1**: Distributed message broker
- **Apache Hadoop 2.8.3**: YARN for resource management
- **RocksDB**: Embedded key-value store for state management
- **Java 8**: Primary programming language
- **Maven 3.0+**: Build and dependency management

## Prerequisites

- Java 8 or higher
- Maven 3.0 or higher
- Apache Kafka cluster (configured with Zookeeper)
- Apache Hadoop/YARN cluster (for production deployment)
- PEM file for EMR deployment (if deploying on AWS EMR)

## Project Structure

```
driver-rider-match/
├── driver-match/
│   ├── pom.xml                          # Maven project configuration
│   ├── runner.sh                        # Deployment script for running the job
│   ├── submitter_task2                  # Binary for task submission
│   ├── references                       # Reference guide template
│   └── src/
│       ├── main/
│       │   ├── java/com/cloudcomputing/samza/nycabs/
│       │   │   ├── DriverMatchTask.java              # Main stream processing task
│       │   │   ├── DriverMatchConfig.java            # Stream configuration
│       │   │   └── application/
│       │   │       └── DriverMatchTaskApplication.java  # Application descriptor
│       │   ├── config/
│       │   │   └── driver-match.properties           # Job configuration
│       │   ├── resources/
│       │   │   └── log4j.xml                         # Logging configuration
│       │   └── assembly/
│       │       └── src.xml                           # Assembly descriptor
│       └── test/
│           └── java/com/cloudcomputing/samza/nycabs/
│               ├── TestDriverMatchTask.java          # Unit tests
│               └── TestUtils.java                    # Test utilities
├── start_kafka.sh                       # Script to deploy Kafka on EMR cluster
├── deploy_task2                         # Binary for deployment
└── README.md                            # This file
```

## Configuration

### Key Configuration Files

#### 1. driver-match.properties

Main configuration file located at `driver-match/src/main/config/driver-match.properties`:

```properties
# Job Configuration
job.factory.class=org.apache.samza.job.yarn.YarnJobFactory
job.name=driver-match
job.coordinator.system=kafka

# TaskApplication class
app.class=com.cloudcomputing.samza.nycabs.application.DriverMatchTaskApplication

# Kafka System Configuration
systems.kafka.samza.factory=org.apache.samza.system.kafka.KafkaSystemFactory
systems.kafka.samza.msg.serde=json

# Update with your cluster details:
systems.kafka.consumer.zookeeper.connect=<master-node>:2181/
systems.kafka.producer.bootstrap.servers=<broker1>:9092,<broker2>:9092,<broker3>:9092

# YARN package path
yarn.package.path=hdfs://<master-node>:8020/nycabs-0.0.1-dist.tar.gz

# State Store Configuration
stores.driver-loc.factory=org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory
stores.driver-loc.changelog=kafka.driver-loc-changelog
stores.driver-loc.key.serde=string
stores.driver-loc.msg.serde=json
```

**Important**: Before deployment, update the following placeholders:
- `<master-node>`: Internal DNS of your master node (e.g., `ip-1-2-3-4.ec2.internal`)
- `<broker1>`, `<broker2>`, `<broker3>`: Internal DNS of all Kafka broker nodes

#### 2. DriverMatchConfig.java

Defines the input and output Kafka streams:

```java
public static final SystemStream DRIVER_LOC_STREAM = new SystemStream("kafka", "driver-locations");
public static final SystemStream EVENT_STREAM = new SystemStream("kafka", "events");
public static final SystemStream MATCH_STREAM = new SystemStream("kafka", "match-stream");
```

## Build Instructions

### 1. Build the Project

```bash
cd driver-match
mvn clean package
```

This will:
- Compile the Java source code
- Run unit tests
- Create a distribution tarball: `target/nycabs-0.0.1-dist.tar.gz`

### 2. Verify Build

Check that the following files are created:
- `target/nycabs-0.0.1.jar`
- `target/nycabs-0.0.1-dist.tar.gz`

## Deployment

### Option 1: Using runner.sh (Recommended)

The `runner.sh` script automates the entire deployment process:

```bash
cd driver-match
./runner.sh
```

This script performs:
1. Creates deployment directory structure
2. Builds the project using Maven
3. Extracts distribution to deployment folder
4. Copies tarball to HDFS
5. Launches the Samza job on YARN

### Option 2: Manual Deployment

```bash
# 1. Build the project
cd driver-match
mvn clean package

# 2. Prepare deployment directory
mkdir -p deploy/samza
tar -xvf target/nycabs-0.0.1-dist.tar.gz -C deploy/samza/

# 3. Upload to HDFS
hadoop fs -copyFromLocal -f target/nycabs-0.0.1-dist.tar.gz /

# 4. Update configuration file
# Edit deploy/samza/config/driver-match.properties with your cluster details

# 5. Run the job
deploy/samza/bin/run-app.sh \
  --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
  --config-path="file://$PWD/deploy/samza/config/driver-match.properties"
```

### Setting up Kafka on EMR

Use the provided `start_kafka.sh` script to deploy Kafka on an EMR cluster:

```bash
./start_kafka.sh
```

The script will:
1. Prompt for your PEM file path
2. Deploy Kafka to all slave nodes in the cluster
3. Configure Kafka brokers
4. Start Kafka services
5. Display the broker list for configuration

**Note**: Save the broker list output - you'll need it for the `driver-match.properties` configuration.

## Testing

### Running Unit Tests

```bash
cd driver-match
mvn test
```

The test suite includes:
- `TestDriverMatchTask`: Tests the matching logic and stream processing
- Uses Samza's in-memory test framework for validation

### Test Data

Tests use simulated data generated by `TestUtils.genStreamData()`:
- Mock driver location updates
- Mock rider event requests
- Validates correct matching output

## Kafka Streams

### Input Streams

1. **driver-locations**
   - Contains GPS location updates from drivers
   - Message format: JSON with driver ID, location coordinates, block ID, availability status
   - Partitioned by block ID for geographical locality

2. **events**
   - Contains rider cab request events
   - Message format: JSON with rider ID, pickup location, block ID, request details
   - Partitioned by block ID to match with local drivers

### Output Stream

**match-stream**
- Contains matched rider-driver pairs
- Message format: JSON with rider ID, driver ID, and matching details
- Can be consumed by downstream services for notifications and dispatch

## Development

### Implementing the Matching Logic

The main logic resides in `DriverMatchTask.java`. Key areas to implement:

1. **Initialize State Store** (in `init()` method):
   ```java
   @Override
   public void init(Context context) throws Exception {
       // Get reference to RocksDB key-value store
       // Initialize data structures for driver tracking
   }
   ```

2. **Process Driver Locations**:
   - Update driver availability in state store
   - Track driver positions by block ID

3. **Process Rider Events**:
   - Query state store for available drivers in the same block
   - Apply matching algorithm (e.g., nearest driver)
   - Send match to output stream

### Local Development

For local development and testing:

1. Set up local Kafka and Zookeeper
2. Update `DriverMatchTaskApplication.java`:
   ```java
   private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = 
       ImmutableList.of("localhost:2181");
   private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = 
       ImmutableList.of("localhost:9092");
   ```

3. Run tests: `mvn test`

## Monitoring

Once deployed, monitor your Samza job:

```bash
# Check YARN application status
yarn application -list

# View application logs
yarn logs -applicationId <application-id>

# Check Kafka topic lag
kafka-consumer-groups.sh --bootstrap-server <broker>:9092 \
  --group driver-match --describe
```

## Troubleshooting

### Common Issues

1. **"ClassNotFoundException" or dependency issues**
   - Ensure all dependencies are in the distribution tarball
   - Check `pom.xml` for correct dependency versions

2. **Kafka connection errors**
   - Verify Zookeeper connection string in properties file
   - Check that Kafka brokers are accessible
   - Ensure firewall rules allow communication

3. **YARN submission failures**
   - Verify HDFS path in `yarn.package.path` is correct
   - Check that tarball was uploaded to HDFS
   - Ensure sufficient YARN resources are available

4. **State store errors**
   - Check RocksDB configuration
   - Ensure changelog topic exists in Kafka
   - Verify replication factor matches cluster size

## References

For implementation details, refer to:
- [Apache Samza Documentation](https://samza.apache.org/learn/documentation/latest/)
- [Samza Hello World Examples](https://github.com/apache/samza-hello-samza)
- [Kafka Streams Documentation](https://kafka.apache.org/documentation/streams/)

## License

This project is part of a cloud computing coursework assignment.

## Contributors

Maintain proper attribution in the `references` file according to academic integrity guidelines.
