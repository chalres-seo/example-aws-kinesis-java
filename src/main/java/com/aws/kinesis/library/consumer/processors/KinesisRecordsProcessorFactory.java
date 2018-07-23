package com.aws.kinesis.library.consumer.processors;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.aws.kinesis.record.handler.HandlerFactory;
import com.aws.kinesis.record.handler.IRecordsHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KinesisRecordsProcessorFactory implements IRecordProcessorFactory {
  private final static Logger logger = LoggerFactory.getLogger(KinesisRecordsProcessorFactory.class);

  private final IRecordsHandler[] handlers;

  private KinesisRecordsProcessorFactory() {
    super();
    this.handlers = null;
  }

  public KinesisRecordsProcessorFactory(IRecordsHandler handler, IRecordsHandler...handlers) {
    this.handlers = HandlerFactory.getInstance().mergeHandler(handler, handlers);
  }

  @Override
  public IRecordProcessor createProcessor() {
    return new KinesisRecordsProcessor(handlers);
  }
}
