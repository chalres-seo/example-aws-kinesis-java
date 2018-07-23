# aws-kinesis-lib-java

Example Java library for AWS Kinesis SDK, KCL, KPL

* This example does not cover advanced topics. (re-sharding, exactly once...)
* All consumers follow at least one processing rule.

## Futures

* AWS Kinesis SDK Stream Management ([SDK Doc] / [SDK Git])
* AWS Kinesis SDK Producer
* AWS Kinesis SDK Consumer
* AWS Kinesis Producer Library Producer ([KPL Doc] / [KPL Git])
* AWS Kinesis Client Library Consumer ([KCL Doc] / [KCL Git])

## Usage

#### SDK Api Client

* Constructor

```Java
  /**
   * Constructor
   *
   * @param awsProfile aws account profile name, or use default from AWS CredentialsFactory
   * @param awsRegion  aws region name. of use default from CredentialsFactory
   * @param kinesisClient aws sdk kinesis client                   
   */
  private ApiClient(final String awsProfile, final String awsRegion, final AmazonKinesisAsync kinesisClient) {
  ...
  }

  public ApiClient(final String awsProfile, final String awsRegion) {
  ...
  }

  public ApiClient() {
  ...
  }
```

* Create and delete stream
* Get stream list
* Check stream status

#### SDK Api Producer

* Constructor

```Java
  /**
   * Constructor
   *
   * @param apiClient aws sdk kinesis client.
   * @param streamName unchecked stream name.
   *
   * @throws ResourceNotFoundException stream is not exist.
   */
  public ApiProducer(final ApiClient apiClient, final String streamName) throws ResourceNotFoundException {
  ...
  }

  public ApiProducer(final String streamName) {
  ...
  }
```

* Produce record interface

```Java
public interface IRecord<T> {
  String getPartitionKey();
  T getValue();
  ByteBuffer getData();
  Optional<String> getSequenceNumber();
}
```

* Produce record

```Java
  /**
   * Produce records.
   *
   * @param records produce records.
   *
   * @return produce result, returns a failure if an error occurs while producing record.
   */
  public boolean produce(final List<IRecord> records) {
  ...
  }
```

* Stop

Produce stop when all records produced or receive interrupt signal

#### SDK Api Consumer

* Constructor

```Java
  /**
   * Constructor
   *
   * @param apiClient aws kinesis sdk client. otherwise create default client.
   * @param streamName unchecked stream name.
   *
   * @throws ResourceNotFoundException stream is not exist.
   */
  public ApiConsumer(@NotNull final ApiClient apiClient, final String streamName) throws ResourceNotFoundException {
  ...
  }

  public ApiConsumer(final String streamName) {
  ...
  }
```

* Consume record

```Java
  /**
   * Start point consumer.
   *
   * Consumer count is equal to shard count * handler count.
   *
   * @param intervalMillis consume interval millis.
   * @param handlers consume records handler list.
   *
   * @return consume loop future list.
   */
  public List<CompletableFuture<Void>> consume(final ShardIteratorType shardIteratorType, final long intervalMillis, final IRecordsHandler handler, final IRecordsHandler... handlers) {
  ...
  }

  public List<CompletableFuture<Void>> consume(@NotNull final ShardIteratorType shardIteratorType, final IRecordsHandler handler, final IRecordsHandler... handlers) {
  ...
  }
```

* Stop

Consumer stop when receive interrupt signal

#### KPL Producer

* Constructor

```Java
  private KplProducer(String profile, String region, String streamName, KinesisProducer kinesisProducer) {
  ...
  }

  public KplProducer(final String profile, final String region, final String streamName) {
  ...
  }

  public KplProducer(final String streamName) {
  ...
  }
```

* Produce record

```Java
  public boolean produce(final List<IRecord> records) {
  ...
  }
```

* Stop

Produce stop when all records produced or receive interrupt signal

#### KCL Consumer

* Constructor

```Java
  /**
   * Constructor
   *
   * @param regionName aws region name.
   * @param streamName aws kinesis stream name.
   * @param appName aws kinesis client library app name. (= dynamodb table name)
   * @param recordProcessorFactory consume record factory.
   * @param awsProfileName aws profile name.
   *
   * @throws UnknownHostException failed create an kinesis client library app-Id.
   */
  private KclConsumer(String regionName,
                     String streamName,
                     String appName,
                     IRecordProcessorFactory recordProcessorFactory,
                     String awsProfileName) throws UnknownHostException {
  ...
  }

  public KclConsumer(String regionName,
                     String streamName,
                     String appName,
                     IRecordProcessorFactory recordProcessorFactory) throws UnknownHostException {
  ...
  }

  public KclConsumer(String streamName,
                     String appName,
                     IRecordProcessorFactory recordProcessorFactory) throws UnknownHostException {
  ...
  }
```

* Consume record

```Java
  public CompletableFuture<Void> consume() {
  ...
  }
```

* Stop

Consumer stop when receive interrupt signal.

```Java
  public Future<Boolean> startGracefulShutdown() {
  ...
  }
```

Before the app shuts down, you can use the gracefullShutdown method to clean up the consumer daemon.


## Build

Unit testing requires AWS credentials.
If you do not have AWS credentials, skip the test.

#### Build

- ./gradlew build

#### Build skip test

- ./gradlew assemble


## Test run

Test process
1. Reading the records from the file (tmp/example_record.txt)
2. Writing them in the kinesis stream (Producer)
3. Re-reading them and outputting to the screen and writing to the file in example dir (Consumer)
4. If the log message is too busy, modify the log level (by logback)

#### Run example by gradlew

- run example library mode : ./gradlew task library
- run example api mode : ./gradlew task api

#### Run example by jar

- run example library mode : java -jar ./build/libs/aws-kinesis-lib-java-1.0-SNAPSHOT.jar library
- run example api mode : java -jar ./build/libs/aws-kinesis-lib-java-1.0-SNAPSHOT.jar api

[SDK API Doc]:  https://docs.aws.amazon.com/kinesis/latest/APIReference/Welcome.html

[SDK Doc]: https://docs.aws.amazon.com/streams/latest/dev/working-with-streams.html
[KCL Doc]: https://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html
[KPL Doc]: https://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-kpl.html

[SDK Git]: https://github.com/aws/aws-sdk-java
[KCL Git]: https://github.com/awslabs/amazon-kinesis-client
[KPL Git]: https://github.com/awslabs/amazon-kinesis-producer
