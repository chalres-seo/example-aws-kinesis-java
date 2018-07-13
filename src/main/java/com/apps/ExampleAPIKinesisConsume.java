package com.apps;

import com.aws.kinesis.api.APIClient;
import com.aws.kinesis.api.consumer.APIConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.*;

public class ExampleAPIKinesisConsume {
  private static Logger logger = LoggerFactory.getLogger(ExampleAPIKinesisProduce.class);

  /**
   * Example AWS Kinesis Consumer App.
   *
   * @param args
   *             0 : "consumer"
   *             1 : profile name.
   *             2 : region name.
   *             3 : stream name.
   */

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    System.out.println("need consumer args : [consumer profile name, region name, stream name]");
    System.out.println("current args : " + Arrays.toString(args));

    final String awsProfileName = args[1];
    final String awsRegionName = args[2];
    final String awsKinesisStreamName = args[3];

    final APIClient kinesisAPIClient;

    if (awsProfileName.equals("default")) {
      kinesisAPIClient = new APIClient();
    } else {
      kinesisAPIClient = new APIClient(awsProfileName, awsRegionName);
    }

    final APIConsumer kinesisAPIConsumer = new APIConsumer(kinesisAPIClient, awsKinesisStreamName);

    System.out.println("Start example kinesis consumer");

    final CompletableFuture<Void> consumerFuture = CompletableFuture
      .runAsync(() -> {
        try {
          kinesisAPIConsumer.consume();
        } catch (ExecutionException | InterruptedException e) {
          e.printStackTrace();
        }
      });

    consumerFuture.get();
  }
}
