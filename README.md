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

```

* Create and delete stream
* Get stream list
* Check stream status

#### SDK Api Producer

* Constructor

```Java

```

* Produce record interface

```Java

```

* Produce record

```Java
def produce[T](records: Vector[RecordImpl[T]]): Future[Boolean]

produce(YOUR_PRODUCE_RECORDS)
```

* Stop

Produce stop when all records produced or receive interrupt signal

#### SDK Api Consumer

* Constructor

```Java

```

* Consume record

```Java

```

* Stop

Consumer stop when receive interrupt signal

#### KPL Producer

* Constructor

```Java

```

* Produce record

```Java

```

* Stop

Produce stop when all records produced or receive interrupt signal

#### KCL Consumer

* Constructor

```Java

```

* Consume record

```Java

```

* Stop

Consumer stop when receive interrupt signal.

```Java

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
