package com.aws.kinesis.consumer;

import static org.hamcrest.CoreMatchers.is;

import com.aws.kinesis.api.APIClient;
import com.aws.kinesis.api.TestAPIConsumeAndProduce;

import com.aws.kinesis.library.producer.KPLProducer;
import com.aws.kinesis.library.producer.KPLProducerFactory;
import com.aws.kinesis.record.ProduceRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


public class TestKPLProducer {
  private final static Logger logger = LoggerFactory.getLogger(TestAPIConsumeAndProduce.class);

  private final String awsProfileName = "default";
  private final String awsRegionName = "ap-northeast-2";
  private final String awsKinesisStreamName = "test-example";
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
  public void testProduce() throws InterruptedException, ExecutionException {
    CompletableFuture<Integer> future = kplProducer.produce(awsKinesisStreamName, exampleRecords);
    kplProducer.flushSync();

    Assert.assertThat(future.get(), is(exampleRecordCount));
  }

}
