package com.aws.kinesis.api.producer;

import com.amazonaws.services.kinesis.model.*;
import com.aws.kinesis.api.ApiClient;
import com.aws.kinesis.record.IRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * AWS SDK Kinesis Producer
 *
 */
public class ApiProducer {
  private static Logger logger = LoggerFactory.getLogger(ApiProducer.class);

  private final ApiClient apiClient;
  private final String streamName;

  /**
   * Constructor
   *
   * @param apiClient aws sdk kinesis client.
   * @param streamName unchecked stream name.
   *
   * @throws ResourceNotFoundException stream is not exist.
   */
  public ApiProducer(final ApiClient apiClient, final String streamName) throws ResourceNotFoundException {
    this.apiClient = apiClient;
    this.streamName = streamName;

    /**
     * chekc stream validate.
     */
    if (apiClient.isNotStreamExist(streamName)) {
      throw new ResourceNotFoundException("failed create apiProducer. stream is not exist, name: " + streamName);
    }
  }

  public ApiProducer(final String streamName) {
    this(new ApiClient(), streamName);
  }

  // Getter >>
  public String getStreamName() { return streamName; }
  // << Getter

  /**
   * Produce records.
   *
   * @param records produce records.
   *
   * @return produce result, returns a failure if an error occurs while producing record.
   */
  public boolean produce(final List<IRecord> records) {
    logger.debug("produce records to stream. name: " + streamName + ", count: " + records.size());

    PutRecordsRequest putRecordsRequest = new PutRecordsRequest()
      .withStreamName(streamName)
      .withRecords(this.toPutRecordsRequestEntryList(records));

    Optional<PutRecordsResult> putRecordsResult = apiClient.putRecords(putRecordsRequest);

    while (true) {
      if (putRecordsResult.isPresent()) {
        if (putRecordsResult.get().getFailedRecordCount() > 0) {
          logger.debug("failed put records. re-produce record, count: " + putRecordsResult.get().getFailedRecordCount());
          putRecordsRequest
            .setRecords(this.getFailedPutRecordsRequestEntryList(putRecordsRequest, putRecordsResult.get()));
          putRecordsResult = apiClient.putRecords(putRecordsRequest);
        } else return true;
      }  else return false;
    }
  }

  private List<PutRecordsRequestEntry> getFailedPutRecordsRequestEntryList(final PutRecordsRequest putRecordsRequest,
                                                                           final PutRecordsResult putRecordsResult) {
    logger.debug("get failed PutRecordsRequestEntryList. " +
      "request count: " + putRecordsRequest.getRecords().size() + ", " +
      "failed count: " + putRecordsResult.getRecords().stream()
        .filter(putRecordsResultEntry -> putRecordsResultEntry.getErrorCode() != null)
        .count());

    final List<PutRecordsRequestEntry> putRecordsRequestEntryList = putRecordsRequest.getRecords();
    final List<PutRecordsResultEntry> putRecordsResultEntryList = putRecordsResult.getRecords();

    final List<PutRecordsRequestEntry> failedPutRecordsRequestEntryList = new ArrayList<>();

    for (int i = 0; i < putRecordsResultEntryList.size(); i++) {
      final PutRecordsRequestEntry putRecordRequestEntry = putRecordsRequestEntryList.get(i);
      final PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList.get(i);
      if (putRecordsResultEntry.getErrorCode() != null) {
        failedPutRecordsRequestEntryList.add(putRecordRequestEntry);
      }
    }

    return failedPutRecordsRequestEntryList;
  }

  private List<PutRecordsRequestEntry> toPutRecordsRequestEntryList(List<IRecord> records) {
    logger.debug("records to PutRecordsRequestEntryList. record count: " + records.size());

    return records.stream().map(this::toPutRecordsRequestEntry).collect(Collectors.toList());
  }

  private PutRecordsRequestEntry toPutRecordsRequestEntry(IRecord record) {
    logger.debug("record to PutRecordsRequestEntry. record: " + record.toString());

    return new PutRecordsRequestEntry()
      .withPartitionKey(record.getPartitionKey())
      .withData(record.getData());
  }
}
