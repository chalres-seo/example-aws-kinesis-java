package com.aws.kinesis.record.handler;

import com.amazonaws.services.kinesis.model.Record;
import com.aws.kinesis.record.IRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class StdoutHandler<T> implements IRecordsHandler<T> {
  private static final Logger logger = LoggerFactory.getLogger(StdoutHandler.class);

  protected StdoutHandler() { super(); }

  @Override
  public void recordsProcess(List<IRecord<T>> records) {
    logger.debug("process records. handler: " + getHandlerType() + ", count: " + records.size());

    for (IRecord record : records) {
      System.out.println(record.toString());
    }
  }

  @Override
  public void kinesisRecordsProcess(List<Record> kinesisRecords) {
    logger.debug("process kinesis records. handler: " + getHandlerType() + ", count: " + kinesisRecords.size());

    for (Record kinesisRecord : kinesisRecords) {
      System.out.println(kinesisRecord.toString());
    }
  }

  @Override
  public HandlerType getHandlerType() {
    return HandlerType.StdoutHandler;
  }

}
