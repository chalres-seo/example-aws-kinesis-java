package com.aws.kinesis.api;

import static org.hamcrest.CoreMatchers.is;

import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class TestAPIClient {
  private final static Logger logger = LoggerFactory.getLogger(TestAPIClient.class);

  private final String testAWSProfileName = "default";
  private final String testAWSRegionName = "ap-northeast-2";
  private final String testAWSKinesisStreamName = "test-example";
  private final int testAWSKinesisShardCount = 1;
  private final String testExceptionAWSKinesisStreamName = "test-exception-example";
  private final String testCreateAWSKinesisStreamName = "test-create-example";

  private final APIClient kinesisAPIClient = new APIClient(testAWSProfileName, testAWSRegionName);

  @Before
  public void setUp() throws InterruptedException, ExecutionException {
    logger.info("check before test streams.");
    if (kinesisAPIClient.isNotStreamExist(testAWSKinesisStreamName)) {
      kinesisAPIClient.createStream(testAWSKinesisStreamName, testAWSKinesisShardCount).get();

      while(kinesisAPIClient.isNotStreamReady(testAWSKinesisStreamName)) {
        logger.debug("wait 5sec for before test stream ready.");
        Thread.sleep(5000);
      }
    }

    if (kinesisAPIClient.isStreamExist(testCreateAWSKinesisStreamName)) {
      kinesisAPIClient.deleteStream(testCreateAWSKinesisStreamName).get();

      while(kinesisAPIClient.isStreamExist(testCreateAWSKinesisStreamName)) {
        logger.debug("wait 5sec for before test stream delete.");
        Thread.sleep(5000);
      }
    }
  }

  @Test
  public void testCreateAndDeleteStream() throws ExecutionException, InterruptedException {
    kinesisAPIClient.createStream(testCreateAWSKinesisStreamName, testAWSKinesisShardCount).get();
    Assert.assertThat(kinesisAPIClient.getStreamStatus(testCreateAWSKinesisStreamName).get(), is("CREATING"));
    kinesisAPIClient.deleteStream(testCreateAWSKinesisStreamName).get();
    Assert.assertThat(kinesisAPIClient.getStreamStatus(testCreateAWSKinesisStreamName).get(), is("DELETING"));
  }

  @Test
  public void testStreamExist() {
    Assert.assertThat(kinesisAPIClient.isStreamExist(testAWSKinesisStreamName), is(true));
    Assert.assertThat(kinesisAPIClient.isNotStreamExist(testAWSKinesisStreamName), is(false));
    Assert.assertThat(kinesisAPIClient.isStreamExist(testExceptionAWSKinesisStreamName), is(false));
    Assert.assertThat(kinesisAPIClient.isNotStreamExist(testExceptionAWSKinesisStreamName), is(true));
  }

  @Test
  public void testStreamList() throws ExecutionException, InterruptedException {
    final Optional<List<String>> streamList = kinesisAPIClient.getStreamList().get();
    streamList.get().forEach(logger::debug);
    Assert.assertThat(streamList.isPresent(), is(true));
    Assert.assertThat(streamList.get().contains(testAWSKinesisStreamName), is(true));
  }

  //@Test(expected= ResourceNotFoundException.class)
  @Test(expected= ExecutionException.class)
  public void testNotExistStream() throws ResourceNotFoundException, ExecutionException, InterruptedException {
    kinesisAPIClient.getShardList(testExceptionAWSKinesisStreamName).get();
  }

  @Test
  public void testShardList() throws ExecutionException, InterruptedException {
    final Optional<List<Shard>> shardList = kinesisAPIClient.getShardList(testAWSKinesisStreamName).get();

    shardList.get().forEach(shard -> logger.debug(shard.getShardId()));

    Assert.assertThat(shardList.get().size(), is(testAWSKinesisShardCount));
  }

  @Test
  public void testShardIteratorList() throws ExecutionException, InterruptedException {
    final Optional<List<String>> shardIteratorList = kinesisAPIClient
        .getShardIterator(testAWSKinesisStreamName, ShardIteratorType.LATEST)
        .get();

    shardIteratorList.get().forEach(logger::debug);

    Assert.assertThat(shardIteratorList.get().size(),is(testAWSKinesisShardCount));
  }
}