package com.aws.kinesis.api.consumer;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.*;
import com.aws.kinesis.api.APIClient;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * AWS SDK Kinesis Consumer API
 */
public class APIConsumer {
  private static Logger logger = LoggerFactory.getLogger(APIConsumer.class);
  
  private final ConcurrentLinkedQueue<String> buffer = new ConcurrentLinkedQueue<>();

  private final AmazonKinesisAsync kinesisClient;
  private final String streamName;

  private final APIClient kinesisAPIClient;
  private AtomicInteger consumerStartedCount = new AtomicInteger(0);

  public APIConsumer(APIClient kinesisAPIClient,
                     String streamName) throws APIClient.StreamIsNotReadyException {
    this.kinesisAPIClient = kinesisAPIClient;
    this.kinesisClient = kinesisAPIClient.getAmazonKinesisClient();
    this.streamName = streamName;

    kinesisAPIClient.checkStreamReadyIfNotThrow(streamName);
  }

  public List<CompletableFuture<Void>> consume() throws APIClient.StreamIsNotReadyException, ExecutionException, InterruptedException {
    return this.consume(1);
  }

  private List<CompletableFuture<Void>> consume(final int intervalTimeSec) throws APIClient.StreamIsNotReadyException, ExecutionException, InterruptedException {
    logger.debug(logMsg("consume records"));
    kinesisAPIClient.checkStreamReadyIfNotThrow(streamName);

    final Optional<List<String>> shardIteratorList =
      kinesisAPIClient.getShardIterator(streamName, ShardIteratorType.LATEST).get();

    if (shardIteratorList.isPresent()) {
      return shardIteratorList.get().stream()
        .map(shardIterator ->
          CompletableFuture
            .runAsync(() -> this.consumeLoop(shardIterator, intervalTimeSec))
            .exceptionally(ex -> {
              logger.debug(logMsg(ex.getMessage()));
              ex.printStackTrace();
              return null;
            }))
        .collect(Collectors.toList());
    } else {
      throw new ExecutionException(new Exception("Failed get shard iterator list"));
    }

  }

  /**
   * Consumer main loop.
   * @param startShardIterator start shard iterator
   * @param intervalTimeSec consume interval sec
   */
  private void consumeLoop(final String startShardIterator, final int intervalTimeSec) {
    logger.debug(logMsg("start consume loop."));
    this.consumerStartedCount.incrementAndGet();

    List<Record> records;
    String nextShardIterator = startShardIterator;

    while (true) {
      final GetRecordsRequest getRecordsRequest = new GetRecordsRequest()
        .withShardIterator(nextShardIterator);

      logger.debug(logMsg("current shard iterator : " + getRecordsRequest.getShardIterator()));

      logger.debug(logMsg("get records"));
      final GetRecordsResult getRecordsResult = kinesisClient.getRecords(getRecordsRequest);
      records = getRecordsResult.getRecords();

      logger.debug(logMsg("process records"));
      if (records.size() > 0) {
        logger.debug(logMsg("process records : " + records.size()));
        final List<CompletableFuture> tasks = this.consumeLoopRecordTasks(records);
        logger.debug(logMsg("tasks run : " + tasks.size()));
      } else {
        logger.debug(logMsg("process records is null"));
      }

      try {
        Thread.sleep(intervalTimeSec * 1000);
      }
      catch (InterruptedException exception) {
        throw new RuntimeException(exception);
      }

      nextShardIterator = getRecordsResult.getNextShardIterator();

      if (nextShardIterator == null) {
        logger.debug(logMsg("next shard iterator is null. stop consuming"));
        break;
      }

      logger.debug(logMsg("next shard iterator : " + nextShardIterator));
    }
  }

  /**
   * consume loop sub tasks
   * @param records tasks target records
   * @return tasks futures
   */
  private List<CompletableFuture> consumeLoopRecordTasks(final List<Record> records) {
    final List<CompletableFuture> tasks = new ArrayList<>();
    final List<String> stringRecords = this.recordToString(records);

    //tasks.add(CompletableFuture.runAsync(() -> this.stdoutRecords(stringRecords)));
    tasks.add(CompletableFuture.runAsync(() -> this.debugRecordPrinter(stringRecords)));
    tasks.add(CompletableFuture.runAsync(() -> this.recordToBuffer(stringRecords)));

    return tasks;
  }

  public boolean isConsumerStarted() {
    return consumerStartedCount.get() > 0;
  }

  public int getStartedConsumerCount() {
    return consumerStartedCount.get();
  }

  private void recordToBuffer(final List<String> stringRecords ) {
    logger.debug(logMsg("record to buffer"));
    buffer.addAll(stringRecords);
  }

  private List<String> recordToString(final List<Record> records) {
    logger.debug(logMsg("record to string"));
    return records.stream().map(record ->
      decodeRecord(record.getData())).collect(Collectors.toList());
  }

  private void debugRecordPrinter(final List<String> records) {
    logger.debug(logMsg("record print : \n" + records + "\n"));
  }

  private void stdoutRecords(final List<String> records) {
    final DateTime dateTimeNow = DateTime.now();
    System.out.println("[" + dateTimeNow + "] " + "consume record : " + records.size());
    records.forEach(record -> System.out.println("[" + dateTimeNow + "] " + record));
  }

  private static String decodeRecord(final ByteBuffer data) {
    return String.valueOf(StandardCharsets.UTF_8.decode(data));
  }

  public synchronized String[] getAllConsumedRecordAndClear() {
    String[] result = new String[buffer.size()];
    buffer.clear();

    return result;
  }

  public synchronized String[] getAllConsumedRecord() {
    return buffer.toArray(new String[buffer.size()]);
  }

  public String getConsumedRecord() {
    return buffer.poll();
  }
  
  public int getBufferSize() { return buffer.size(); }

  private String logMsg(final String msg) {
    return kinesisAPIClient.debugStreamPrefix(streamName) + msg;
  }
}
