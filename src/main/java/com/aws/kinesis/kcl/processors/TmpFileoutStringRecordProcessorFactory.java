package com.aws.kinesis.kcl.processors;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

public class TmpFileoutStringRecordProcessorFactory implements IRecordProcessorFactory {
  public TmpFileoutStringRecordProcessorFactory() { super(); }

  @Override
  public IRecordProcessor createProcessor() {
    return new TmpFileoutIStringRecordProcessor();
  }
}
