package com.aws.kinesis.record;

import java.nio.ByteBuffer;
import java.util.Optional;

public interface IRecord<T> {
  String getPartitionKey();
  T getValue();
  ByteBuffer getData();
  Optional<String> getSequenceNumber();
}
