package com.aws.kinesis.api.producer;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.aws.kinesis.api.APIClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * AWS SDK Kinesis Producer API
 */
public class APIProducer {
  private static Logger logger = LoggerFactory.getLogger(APIProducer.class);

  private final AmazonKinesisAsync kinesisClient;
  private final String streamName;

  private final APIClient kinesisAPIClient;

  public APIProducer(APIClient kinesisAPIClient,
                     String streamName) throws APIClient.StreamIsNotReadyException {
    this.kinesisAPIClient = kinesisAPIClient;
    this.kinesisClient = kinesisAPIClient.getAmazonKinesisClient();
    this.streamName = streamName;

    kinesisAPIClient.checkStreamReadyIfNotThrow(streamName);
  }

  public CompletableFuture<Void> produce(final Stream<String> records)  throws APIClient.StreamIsNotReadyException {
    this.debug("produce records");
    kinesisAPIClient.checkStreamReadyIfNotThrow(streamName);

    return CompletableFuture.runAsync(() -> {
      List<PutRecordsRequestEntry> putRecordsRequestEntryList = records.map(record ->
        new PutRecordsRequestEntry()
          .withData(this.encodeRecord(record))
          .withPartitionKey("partitionKey-" + record)).collect(Collectors.toList());

      PutRecordsRequest putRecordsRequest = new PutRecordsRequest()
        .withStreamName(streamName)
        .withRecords(putRecordsRequestEntryList);

      this.debug("put records : " + putRecordsRequestEntryList.size());
      PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);

      while (putRecordsResult.getFailedRecordCount() > 0) {
        this.debug("handling failures records : " + putRecordsResult.getFailedRecordCount());

        final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
        final List<PutRecordsResultEntry> putRecordsResultEntryList = putRecordsResult.getRecords();

        for (int i = 0; i < putRecordsResultEntryList.size(); i++) {
          final PutRecordsRequestEntry putRecordRequestEntry = putRecordsRequestEntryList.get(i);
          final PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList.get(i);
          if (putRecordsResultEntry.getErrorCode() != null) {
            failedRecordsList.add(putRecordRequestEntry);
          }
        }

        putRecordsRequestEntryList = failedRecordsList;
        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
      }

      this.debug("put records complete.");
    });
  }

  public Stream<String> createExampleStringRecords(int recordCount) {
    this.debug("create example records : " + recordCount);

    final Stream.Builder<String> streamBuilder = Stream.builder();

    for(int i = 0; i < recordCount; i++) {
      //streamBuilder.add("keyrecord_" + i);
      streamBuilder.add(String.format("{\"key\" : \"key_%d\", " +
          "\"string_value\" : \"string_value_%d\", " +
          "\"numeric_value\" : %d}", i, i, i));
    }

    return streamBuilder.build();
  }

  private ByteBuffer encodeRecord(String record) {
    return StandardCharsets.UTF_8.encode(record);
  }

  private void debug(String msg) {
    logger.debug(kinesisAPIClient.debugStreamPrefix(streamName) + msg);
  }
}
