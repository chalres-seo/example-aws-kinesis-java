package com.aws.kinesis.kcl;

import com.aws.kinesis.api.APIClient;
import com.aws.kinesis.kcl.processors.StdoutStringRecordProcessorFactory;
import com.aws.kinesis.kcl.processors.TmpFileoutStringRecordProcessorFactory;
import com.aws.kinesis.kpl.KPLProducer;
import com.aws.kinesis.kpl.KPLProducerFactory;
import com.aws.kinesis.record.ProduceRecord;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.is;

public class TestKCLConsumer {
  private final static Logger logger = LoggerFactory.getLogger(TestKCLConsumer.class);

  private final String awsProfileName = "default";
  private final String awsRegionName = "ap-northeast-2";
  private final String awsKinesisStreamName = "test-example";
  private final String awsKinesisConsumerAppName = "test-consumer";
  private final int awsKinesisStreamShardCount = 1;
  private final int exampleRecordCount = 10;

  private final APIClient kinesisAPIClient = new APIClient(awsProfileName, awsRegionName);
  private final KPLProducer kplProducer = KPLProducerFactory.getInstance().getKPLProducer(awsProfileName, awsRegionName);

  private final List<ProduceRecord> exampleRecords =  kinesisAPIClient.createExampleStringRecords(exampleRecordCount);

  @Before
  public void setUp() throws InterruptedException, ExecutionException {
    logger.info("check before test streams.");
    if (kinesisAPIClient.isNotStreamExist(awsKinesisStreamName)) {
      kinesisAPIClient.createStream(awsKinesisStreamName, awsKinesisStreamShardCount).get();

      while(kinesisAPIClient.isNotStreamReady(awsKinesisStreamName)) {
        logger.debug("wait 5sec for before test stream ready.");
        Thread.sleep(5000);
      }
    }
  }

  @Test
  public void testConsumeStdout() throws InterruptedException, ExecutionException, UnknownHostException {
    final KCLConsumer kclConsumer = new KCLConsumer(awsRegionName,
      awsKinesisStreamName,
      awsKinesisConsumerAppName,
      new StdoutStringRecordProcessorFactory());

    final Future consumefuture = kclConsumer.consume();
    logger.info("start consume record 30 sec.");
    Thread.sleep(30000);
    this.produceSampleRecord();
    Thread.sleep(5000);

    if (!kclConsumer.hasGracefulShutdownStarted()) {
      logger.info("start consumer graceful shutdown");
      kclConsumer.startGracefulShutdown().get();
    }
  }

  @Test
  public void testConsumeTmpFileout() throws UnknownHostException, InterruptedException, ExecutionException {
    final KCLConsumer kclConsumer = new KCLConsumer(awsRegionName,
      awsKinesisStreamName,
      awsKinesisConsumerAppName,
      new TmpFileoutStringRecordProcessorFactory());

    final Future consumefuture = kclConsumer.consume();
    logger.info("start consume record 30 sec.");
    Thread.sleep(20000);
    this.produceSampleRecord();
    Thread.sleep(30000);

    if (!kclConsumer.hasGracefulShutdownStarted()) {
      logger.info("start consumer graceful shutdown");
      kclConsumer.startGracefulShutdown().get();
    }
  }

  private void produceSampleRecord() throws InterruptedException, ExecutionException {
    kplProducer.produce(awsKinesisStreamName, exampleRecords).get();
    kplProducer.flushSync();
  }
}
