package com.aws.kinesis.library.consumer.processors;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.model.Record;

import java.util.List;

public interface IStringRecordProcessorImpl extends IRecordProcessor {
  /**
   * Process records performing retries as needed. Skip "poison pill" records.
   *
   * @param records Data records to be processed.
   */
  public void processRecordsWithRetries(List<Record> records);

  /**
   * Process a single record.
   *
   * @param record The record to be processed.
   */
  public void processSingleRecord(Record record);
}
