package com.aws.kinesis.library;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.aws.kinesis.api.ApiClient;
import com.aws.kinesis.api.TestApiClient;
import com.aws.kinesis.library.consumer.KclConsumer;
import com.aws.kinesis.library.consumer.processors.KinesisRecordsProcessorFactory;
import com.aws.kinesis.library.producer.KplProducer;
import com.aws.kinesis.record.StringRecord;
import com.aws.kinesis.record.handler.HandlerFactory;
import com.aws.kinesis.record.handler.HandlerType;
import com.aws.kinesis.record.handler.StdoutHandler;
import com.utils.AppConfig;
import com.utils.AppUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.is;

public class TestKclConsumerAndKplProduer {
  private final static Logger logger = LoggerFactory.getLogger(TestKclConsumerAndKplProduer.class);

  private String testStreamName = "test-stream";
  private String tmpFilePathString = "tmp/file.out";
  private int testProduceRecordCount = 10;

  private String testConsumerAppName = "test-stream-consumer-app";

  private int waitSec = 5;
  private long waitMillis = 5000L;

  private ApiClient apiClient = new ApiClient();

  final AmazonDynamoDB dynamodbClient = AmazonDynamoDBClientBuilder.standard()
    .withRegion(AppConfig.getAwsRegion())
    .build();

  public void setup() {
    cleanUp();

    if (apiClient.isNotStreamExist(testStreamName)) {
      do {
        apiClient.createStream(testStreamName);
      } while(!apiClient.waitStreamReady(testStreamName));
    }
  }

  public void cleanUp() {
    if (apiClient.isStreamExist(testStreamName)) {
      do {
        apiClient.deleteStream(testStreamName);
      } while(!apiClient.waitStreamDelete(testStreamName));
    }

    try {
      logger.debug("delete tmp file.");
      Files.deleteIfExists(Paths.get(tmpFilePathString));
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (dynamodbClient.listTables().getTableNames().contains(testConsumerAppName)) {
      logger.debug("delete dynamodb");
      dynamodbClient.deleteTable(testConsumerAppName);

      while(!dynamodbClient.listTables().getTableNames().contains(testConsumerAppName)) {
          AppUtils.backoff("delete dynamodb table.");
      }
    }
  }

  @Test
  public void test99CleanUpResource() {
    cleanUp();
  }

  @Test
  public void test01KclConsumeAndKplProduce() throws InterruptedException, IOException, ExecutionException {
    this.setup();

    HandlerFactory handlerFactory = HandlerFactory.getInstance();

    KplProducer kplProducer = new KplProducer(testStreamName);

    KclConsumer kclConsumer = new KclConsumer(testStreamName, testConsumerAppName,
      new KinesisRecordsProcessorFactory(
        handlerFactory.getHandler(HandlerType.DebugoutHandler),
        handlerFactory.getHandler(HandlerType.StdoutHandler),
        handlerFactory.getHandler(HandlerType.TmpFileoutHandler, tmpFilePathString)));

    CompletableFuture<Void> kclConsumerFuture = kclConsumer.consume();

    Thread.sleep(waitMillis * 6L);

    kplProducer.produce(AppUtils.createExampleRecords(testProduceRecordCount));

    Thread.sleep(waitMillis * 6L);

    Future kclConsumerShutDownFuture = kclConsumer.startGracefulShutdown();
    kclConsumerShutDownFuture.get();
    //Thread.sleep(waitMillis * 6L);

    Assert.assertThat((int) Files.newBufferedReader(Paths.get(tmpFilePathString)).lines().count(), is(testProduceRecordCount));
  }
}
