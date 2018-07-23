package com.aws.kinesis.library.producer;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.aws.credentials.CredentialsFactory;
import com.aws.kinesis.record.IRecord;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.sun.istack.internal.Nullable;
import com.utils.AppConfig;
import com.utils.AppUtils;
import com.utils.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public final class KplProducer {
  private static final Logger logger = LoggerFactory.getLogger(KplProducer.class);

  private static final KinesisProducerConfiguration kinesisProducerConfiguration = AppConfig.getKplDaemonProps();

  private final String profile;
  private final String region;
  private final String streamName;
  private final KinesisProducer kinesisProducer;

  private KplProducer(String profile, String region, String streamName, KinesisProducer kinesisProducer) {
    this.profile = profile;
    this.region = region;
    this.streamName = streamName;
    this.kinesisProducer = kinesisProducer;
  }

  public KplProducer(final String profile, final String region, final String streamName) {
    this(profile, region, streamName,
      new KinesisProducer(kinesisProducerConfiguration
        .setRegion(region)
        .setCredentialsProvider(CredentialsFactory.getInstance().getCredentialsProvider(profile))));
  }

  public KplProducer(final String streamName) {
    this(AppConfig.getAwsProfile(), AppConfig.getAwsRegion(), streamName,
      new KinesisProducer(kinesisProducerConfiguration
        .setRegion(AppConfig.getAwsRegion())
        .setCredentialsProvider(CredentialsFactory.getInstance().getCredentialsProvider(AppConfig.getAwsProfile()))));
  }

  private ListenableFuture<UserRecordResult> produceSingleRecord(final IRecord record, @Nullable final FutureCallback<UserRecordResult> callback) {
    logger.debug("add user record. stream: " + streamName + ", record: " + record.toString());

    ListenableFuture<UserRecordResult> addUserRecordFuture = kinesisProducer.addUserRecord(streamName, record.getPartitionKey(), record.getData());

    if (callback == null) {
      return addUserRecordFuture;
    } else {
      Futures.addCallback(addUserRecordFuture, callback);
      return addUserRecordFuture;
    }
  }

  private ListenableFuture<UserRecordResult> produceSingleRecord(final IRecord record) {
    return this.produceSingleRecord(record, null);
  }

  private List<IRecord> getFailedRecords(final List<Tuple2<IRecord, ListenableFuture<UserRecordResult>>> produceRecordFutures) {
    logger.debug("get failed produce records.");

    return produceRecordFutures.stream()
      .filter(future -> {
        try {
          final UserRecordResult userRecordResult = future.getRear().get();
          if (userRecordResult.isSuccessful()) {
            logger.debug("add record succeed.");
          } else {
            logger.error("failed add record. name: " + streamName + ", record: " + future.getForward());
            logger.error(userRecordResult.getAttempts().toString());
          }
          return !userRecordResult.isSuccessful();
        } catch (InterruptedException | ExecutionException e) {
          logger.error("failed wait for get result add record. future get exception, name: " + streamName + ", record: " + future.getForward());
          logger.error(e.getMessage());
          return true;
        }
      }).map(Tuple2::getForward).collect(Collectors.toList());
  }

  private List<Tuple2<IRecord, ListenableFuture<UserRecordResult>>> produceRecords(final List<IRecord> records) {
    logger.debug("produce records. stream name: " + streamName + ", records count:" + records.size());

    List<Tuple2<IRecord, ListenableFuture<UserRecordResult>>> produceRecordsFutures = new ArrayList<>(records.size());

    for (IRecord record : records) {
      produceRecordsFutures.add(new Tuple2<>(record, this.produceSingleRecord(record)));
    }

    return produceRecordsFutures;
  }

  public boolean produce(final List<IRecord> records) {
    logger.debug("produce records with retry. stream name: " + streamName + ", record count: " + records.size());

    List<IRecord> currentRecords = records;

    for (int i = 0; i < AppConfig.getRetryAttemptCount(); i++) {
      currentRecords = this.getFailedRecords(this.produceRecords(currentRecords));

      if (currentRecords.isEmpty()) {
        logger.debug("produce all records succeed. flush sync and finish.");
        kinesisProducer.flushSync();
        return true;
      }
      logger.debug("failed records is not empty. backoff and flush sync and retry to produce remain record. " +
        "attemps: " + i + "/" + AppConfig.getRetryAttemptCount() + ", " +
        "stream: " + streamName + ", " +
        "records count: " + currentRecords.size());
      AppUtils.backoff("retry to produce remain record.");
      kinesisProducer.flushSync();
    }
    logger.error("failed produce records. exceed retry attempts. " +
      "stream: " + streamName + ", " +
      "remain record count: " + currentRecords.size());

    return false;
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
}