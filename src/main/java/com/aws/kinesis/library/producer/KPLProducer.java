package com.aws.kinesis.library.producer;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.aws.kinesis.record.ProduceRecord;
import com.google.common.util.concurrent.ListenableFuture;
import com.utils.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public final class KPLProducer {
  private static Logger logger = LoggerFactory.getLogger(KPLProducer.class);

  // Backoff and retry settings
  private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
  private static final int NUM_RETRIES = 10;

  // custom kpl producer (profile, region)
  final private KinesisProducer kinesisProducer;

  KPLProducer(KinesisProducer kinesisProducer) {
    kinesisProducer.flush();
    this.kinesisProducer = kinesisProducer;
  }

  public CompletableFuture<Integer> produce(final String streamName, final List<ProduceRecord> produceRecords) {
    return CompletableFuture.supplyAsync(() -> this.produceRecords(streamName, produceRecords));
  }

  /**
   * Produce records retries as needed
   *
   * @param streamName producing target stream name
   * @param produceRecords producing records
   * @return successfully producing record count
   */
  private int produceRecords(final String streamName, final List<ProduceRecord> produceRecords) {
    logger.debug("produce records");

    final int produceRecordCount = produceRecords.size();
    logger.debug("produce records count : " + produceRecordCount);

    int failedProduceRecordCount = produceRecordCount;
    boolean produceSuccessfully = false;

    List<Tuple2<ProduceRecord, ListenableFuture<UserRecordResult>>> failedProduceFutureTuples =
      checkSuccessFullyProduceRecords(streamName,
        produceRecords
          .stream()
          .map(produceRecord -> this.produceSingleRecord(streamName, produceRecord))
          .collect(Collectors.toList()));

    for (int i = 0; i < NUM_RETRIES; i++) {
      try {
        logger.debug("produce records try count : " + i);
        // Logic to produce records.
        failedProduceFutureTuples = checkSuccessFullyProduceRecords(streamName, failedProduceFutureTuples);

        failedProduceRecordCount = failedProduceFutureTuples.size();
        logger.debug("produce records failed count : " + failedProduceRecordCount);

        if (failedProduceRecordCount == 0) {
          logger.debug("produce records successfully complete.");
          produceSuccessfully = true;
          break;
        }
      } catch (Throwable t) {
        logger.warn("Caught throwable while processing record " + failedProduceFutureTuples, t);
      }
      // backoff if we encounter an exception.
      logger.debug("Backoff due to failed produce.");
      backoff();
    }

    if (!produceSuccessfully) {
      logger.error("Couldn't produce record. Skipping the record.");
      logger.error("skipped record : " + failedProduceFutureTuples);
    }
    return produceRecordCount - failedProduceRecordCount;
  }

  private Tuple2<ProduceRecord, ListenableFuture<UserRecordResult>> produceSingleRecord(String streamName,
                                                                                        ProduceRecord produceRecord) {
    logger.debug("produce record : " + produceRecord.getString());
    return new Tuple2<>(produceRecord,
      kinesisProducer.addUserRecord(streamName, produceRecord.getPartitionKey(), produceRecord.getByteBuffer()));
  }

  private List<Tuple2<ProduceRecord, ListenableFuture<UserRecordResult>>> checkSuccessFullyProduceRecords(
    final String streamName,
    final List<Tuple2<ProduceRecord, ListenableFuture<UserRecordResult>>> produceFutureTuples) {

    logger.debug("check successfully produce records");
    return produceFutureTuples.stream().map(tuple -> {
      try {
        if (tuple.getRear().get().isSuccessful()) {
          logger.debug("record success : " + tuple.getForward().getString());
          return null;
        } else {
          logger.debug("record failed.");
          logger.debug("retry produce record : " + tuple.getForward().getString());
          return this.produceSingleRecord(streamName, tuple.getForward());
        }
      } catch (InterruptedException | ExecutionException e) {
        logger.debug("record exceptionally.");
        logger.debug("retry produce record : " + tuple.getForward().getString());
        return this.produceSingleRecord(streamName, tuple.getForward());
      }
    }).filter(Objects::nonNull).collect(Collectors.toList());
  }

  public void flush() {
    kinesisProducer.flush();
  }

  public void flush(String streamName) {
    kinesisProducer.flush(streamName);
  }

  public void flushSync() {
    kinesisProducer.flushSync();
  }

  private void backoff() {
    logger.debug("backoff in millis : " + BACKOFF_TIME_IN_MILLIS);
    try {
      Thread.sleep(BACKOFF_TIME_IN_MILLIS);
    } catch (InterruptedException e) {
      logger.debug("Interrupted sleep", e);
    }
  }
}
