package com.aws.kinesis.record.handler;

import com.amazonaws.services.kinesis.model.Record;
import com.aws.kinesis.record.IRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class DebugoutHandler<T> implements IRecordsHandler<T> {
  private static final Logger logger = LoggerFactory.getLogger(DebugoutHandler.class);

  protected DebugoutHandler() { super(); }

  @Override
  public void recordsProcess(List<IRecord<T>> records) throws IOException {
    logger.debug("process records. handler: " + getHandlerType() + ", count: " + records.size());

    for (IRecord record : records) {
      logger.debug(record.toString());
    }
  }

  @Override
  public void kinesisRecordsProcess(List<Record> kinesisRecords) {
    logger.debug("process kinesis records. handler: " + getHandlerType() + ", count: " + kinesisRecords.size());

    for (Record kinesisRecord : kinesisRecords) {
      logger.debug(kinesisRecord.toString());
    }
  }

  @Override
  public HandlerType getHandlerType() {
    return HandlerType.DebugoutHandler;
  }

}
