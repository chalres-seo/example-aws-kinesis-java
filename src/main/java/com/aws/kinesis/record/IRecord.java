package com.aws.kinesis.record;

import com.amazonaws.services.kinesis.model.Record;

import java.nio.ByteBuffer;
import java.util.Optional;

public interface IRecord<T> {
  String getPartitionKey();
  T getData();
  ByteBuffer getByteBuffer();
  Optional<String> getSequenceNumber();
  Record getKinesisRecord();
}
