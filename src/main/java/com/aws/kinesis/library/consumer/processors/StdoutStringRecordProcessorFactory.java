package com.aws.kinesis.library.consumer.processors;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

public class StdoutStringRecordProcessorFactory implements IRecordProcessorFactory {
  public StdoutStringRecordProcessorFactory() {
    super();
  }

  @Override
  public IRecordProcessor createProcessor() {
    return new StdoutIStringRecordProcessor();
  }
}
