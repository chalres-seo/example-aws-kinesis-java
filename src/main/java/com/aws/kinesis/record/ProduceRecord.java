package com.aws.kinesis.record;

import java.nio.ByteBuffer;

public interface ProduceRecord<A> {
  public ByteBuffer getByteBuffer();
  public String getPartitionKey();
  public A getRecord();
  public String getString();
}
