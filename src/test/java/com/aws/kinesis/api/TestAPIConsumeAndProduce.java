package com.aws.kinesis.api;

import static org.hamcrest.CoreMatchers.is;

import com.aws.kinesis.api.consumer.APIConsumer;
import com.aws.kinesis.api.producer.APIProducer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;


public class TestAPIConsumeAndProduce {
  private final static Logger logger = LoggerFactory.getLogger(TestAPIConsumeAndProduce.class);

  private final String awsProfileName = "default";
  private final String awsRegionName = "ap-northeast-2";
  private final String awsKinesisStreamName = "test-example";
  private final String awsKinesisStreamShardCount = "test-example";
  private final int exampleRecordCount = 10;

  private final APIClient kinesisAPIClient = new APIClient(awsProfileName, awsRegionName);

  private final APIConsumer apiConsumer = new APIConsumer(kinesisAPIClient, awsKinesisStreamName);
  private final APIProducer apiProducer = new APIProducer(kinesisAPIClient, awsKinesisStreamName);

  @Before
  public void setUp() throws InterruptedException, ExecutionException {
    logger.info("check before test streams.");
    if (kinesisAPIClient.isNotStreamExist(awsKinesisStreamName)) {
      kinesisAPIClient.createStream(awsKinesisStreamName, 1).get();

      while(kinesisAPIClient.isNotStreamReady(awsKinesisStreamName)) {
        logger.debug("wait 5sec for before test stream ready.");
        Thread.sleep(5000);
      }
    }
  }

  @Test
  public void testRun() throws ExecutionException, InterruptedException {
    CompletableFuture<List<CompletableFuture<Void>>> consumerFuture = CompletableFuture
      .supplyAsync(() -> {
        try {
          return this.runConsumer();
        } catch (ExecutionException | InterruptedException e) {
          e.printStackTrace();
          return null;
        }
      });

    logger.debug("wait for consumer start consume");
    while(!apiConsumer.isConsumerStarted()) { Thread.sleep(1000); }

    CompletableFuture<Void> produceFuture = this.runProducer();

    consumerFuture.get();

    Assert.assertThat(apiConsumer.getAllConsumedRecord().length, is(exampleRecordCount));
    Assert.assertThat(apiConsumer.getBufferSize(), is(exampleRecordCount));;
    Assert.assertThat(apiConsumer.getAllConsumedRecordAndClear().length, is(exampleRecordCount));
    Assert.assertThat(apiConsumer.getBufferSize(), is(0));
  }

  private CompletableFuture<Void> runProducer() {
    logger.debug("test example produce.");
    final Stream<String> exampleRecords =  apiProducer.createExampleStringRecords(exampleRecordCount);

    return apiProducer.produce(exampleRecords);
  }

  private List<CompletableFuture<Void>> runConsumer() throws ExecutionException, InterruptedException {
    logger.debug("test consumer run for 9 sec.");
    final List<CompletableFuture<Void>> consumerFutures = apiConsumer.consume();

    int timeoutCount = 3;

    logger.debug("check consume works every 3 sec.");
    while(!consumerFutures.stream().allMatch(Future::isDone)) {
      logger.debug("check consumers isDone: " + consumerFutures.size());
      consumerFutures.forEach(future -> logger.debug(String.valueOf(future.isDone())));
      logger.debug("check consumers isCancelled: " + consumerFutures.size());
      consumerFutures.forEach(future -> logger.debug(String.valueOf(future.isCancelled())));
      try {
        Thread.sleep(3000);
      }
      catch (InterruptedException exception) {
        throw new RuntimeException(exception);
      }

      timeoutCount--;

      if(timeoutCount < 0) {
        logger.debug("consumer stop");
        consumerFutures.forEach(future -> future.cancel(false));
        logger.debug("wait consumer stopping 3 sec");
        try {
          Thread.sleep(3000);
        }
        catch (InterruptedException exception) {
          throw new RuntimeException(exception);
        }

      }
    }

    return consumerFutures;
  }
}
