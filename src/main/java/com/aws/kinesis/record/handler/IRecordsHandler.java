package com.aws.kinesis.record.handler;

import com.amazonaws.services.kinesis.model.Record;
import com.aws.kinesis.record.IRecord;

import java.io.IOException;
import java.util.List;

public interface IRecordsHandler<T> {
  void recordsProcess(List<IRecord<T>> records) throws IOException;
  void kinesisRecordsProcess(List<Record> kinesisRecords);
  HandlerType getHandlerType();
}
