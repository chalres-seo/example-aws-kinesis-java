package com.aws.kinesis.api;

import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.aws.kinesis.api.consumer.ApiConsumer;
import com.aws.kinesis.api.producer.ApiProducer;
import com.aws.kinesis.record.handler.HandlerFactory;
import com.aws.kinesis.record.handler.HandlerType;
import com.utils.AppUtils;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.is;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestApiClient {
  private final static Logger logger = LoggerFactory.getLogger(TestApiClient.class);

  private String testStreamName = "test-stream";
  private String tmpFilePathString = "tmp/file.out";
  private int testProduceRecordCount = 10;

  private int waitSec = 5;
  private long waitMillis = 5000L;

  private ApiClient apiClient = new ApiClient();

  public void setup() {
    if (apiClient.isNotStreamExist(testStreamName)) {
      do {
        apiClient.createStream(testStreamName);
      } while(!apiClient.waitStreamReady(testStreamName));
    }

    try {
      Files.deleteIfExists(Paths.get(tmpFilePathString));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void cleanUp() {
    if (apiClient.isStreamExist(testStreamName)) {
      do {
        apiClient.deleteStream(testStreamName);
      } while(!apiClient.waitStreamDelete(testStreamName));
    }

    try {
      Files.deleteIfExists(Paths.get(tmpFilePathString));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test99CleanUpResource() {
    cleanUp();
  }

  @Test
  public void test01StreamIsExist() {
    cleanUp();

    Assert.assertThat(apiClient.isStreamExist(testStreamName), is(false));
    Assert.assertThat(apiClient.getStreamList().contains(testStreamName), is(false));
  }

  @Test
  public void test02StreamCreateAndWaitReady() {
    this.cleanUp();

    Assert.assertThat(apiClient.isStreamReady(testStreamName), is(false));
    Assert.assertThat(apiClient.waitStreamReady(testStreamName), is(false));

    Assert.assertThat(apiClient.createStream(testStreamName), is(true));
    Assert.assertThat(apiClient.createStream(testStreamName), is(true));

    Assert.assertThat(apiClient.isStreamExist(testStreamName), is(true));
    Assert.assertThat(apiClient.isStreamReady(testStreamName), is(false));

    Assert.assertThat(apiClient.waitStreamReady(testStreamName), is(true));
    Assert.assertThat(apiClient.waitStreamReady(testStreamName), is(true));

    Assert.assertThat(apiClient.isStreamReady(testStreamName), is(true));
  }

  @Test
  public void test03StreamList() {
    this.setup();

    Assert.assertThat(apiClient.getStreamList().contains(testStreamName + "_error"), is(false));
    Assert.assertThat(apiClient.getStreamList().contains(testStreamName), is(true));

  }

  @Test
  public void test04StreamDeleteAndWait() {
    this.setup();

    Assert.assertThat(apiClient.waitStreamDelete(testStreamName), is(false));

    Assert.assertThat(apiClient.deleteStream(testStreamName), is(true));
    Assert.assertThat(apiClient.deleteStream(testStreamName), is(true));

    Assert.assertThat(apiClient.isStreamReady(testStreamName), is(false));
    Assert.assertThat(apiClient.isStreamExist(testStreamName), is(true));

    Assert.assertThat(apiClient.waitStreamDelete(testStreamName), is(true));
    Assert.assertThat(apiClient.waitStreamDelete(testStreamName), is(true));

    Assert.assertThat(apiClient.isStreamReady(testStreamName), is(false));
    Assert.assertThat(apiClient.isStreamExist(testStreamName), is(false));
  }

  @Test
  public void test05GetShardList() {
    this.setup();

    List<Shard> shardList = apiClient.getShardList(testStreamName);
    Assert.assertThat(shardList.size(), is(1));

    List<String> shardIteratorList = apiClient.getShardIteratorList(testStreamName, ShardIteratorType.LATEST);
    Assert.assertThat(shardIteratorList.size(), is(1));
  }

  @Test
  public void test06APIConsumeAndProduce() throws InterruptedException, IOException {
    this.setup();

    final ApiConsumer apiConsumer = new ApiConsumer(testStreamName);
    final ApiProducer apiProducer = new ApiProducer(testStreamName);

    final HandlerFactory handlerFactory = HandlerFactory.getInstance();

    List<CompletableFuture<Void>> consumerFutures = apiConsumer
      .consume(ShardIteratorType.LATEST,
        handlerFactory.getHandler(HandlerType.DebugoutHandler),
        handlerFactory.getHandler(HandlerType.StdoutHandler),
        handlerFactory.getHandler(HandlerType.TmpFileoutHandler, tmpFilePathString)
      );

    Thread.sleep(waitMillis);

    logger.debug("produce test record. result: " +
      apiProducer.produce(AppUtils.createExampleRecords(testProduceRecordCount)));

    Thread.sleep(waitMillis);

    Assert.assertThat((int) Files.newBufferedReader(Paths.get(tmpFilePathString)).lines().count(), is(testProduceRecordCount));
  }
}
