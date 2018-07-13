package com.apps;

import com.aws.kinesis.api.APIClient;
import com.aws.kinesis.api.producer.APIProducer;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ExampleAPIKinesisProduce {
  private static Logger logger = LoggerFactory.getLogger(ExampleAPIKinesisProduce.class);

  /**
   * Example AWS Kinesis Producer App.
   *
   * @param args
   *             0 : "producer"
   *             1 : profile name.
   *             2 : region name.
   *             3 : stream name.
   *             4 : example record interval sec
   *             5 : example record count per arg[4] sec.
   */

  public static void main(String[] args) {
    System.out.println("need consumer args : [producer profile name, " +
        "region name, " +
        "stream name, " +
        "produce example record interval sec, " +
        "produce example record count per interval sec]");
    System.out.println("current args : " + Arrays.toString(args));

    final String awsProfileName = args[1];
    final String awsRegionName = args[2];
    final String awsKinesisStreamName = args[3];
    final int exampleProduceRecordInterval = Integer.valueOf(args[4]);
    final int exampleRecordCount = Integer.valueOf(args[5]);

    final APIClient kinesisAPIClient;

    if (awsProfileName.equals("default")) {
      kinesisAPIClient = new APIClient();
    } else {
      kinesisAPIClient = new APIClient(awsProfileName, awsRegionName);
    }

    final APIProducer kinesisAPIProducer = new APIProducer(kinesisAPIClient, awsKinesisStreamName);

    final List<String> exampleRecords =
      kinesisAPIProducer.createExampleStringRecords(exampleRecordCount).collect(Collectors.toList());

    final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    System.out.println("Start example kinesis producer.");

    executor.scheduleAtFixedRate(() -> {
      final DateTime dateTimeNow = DateTime.now();
      System.out.println("[" + dateTimeNow + "] " + "produce record : " + exampleRecordCount);
      kinesisAPIProducer.produce(exampleRecords.stream());
    }, 0, exampleProduceRecordInterval, TimeUnit.SECONDS);
  }
}
