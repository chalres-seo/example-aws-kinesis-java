package com.aws.kinesis.library.consumer.processors;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

public class TmpFileoutStringRecordProcessorFactory implements IRecordProcessorFactory {
  public TmpFileoutStringRecordProcessorFactory() { super(); }

  @Override
  public IRecordProcessor createProcessor() {
    return new TmpFileoutIStringRecordProcessor();
  }
}
