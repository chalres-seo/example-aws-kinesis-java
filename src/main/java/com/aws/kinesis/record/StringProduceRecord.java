package com.aws.kinesis.record;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;
import java.util.Objects;

public class StringProduceRecord implements ProduceRecord<String> {
  private static Logger logger = LoggerFactory.getLogger(StringProduceRecord.class);

  private String partitionKey;
  private String record;
  private ByteBuffer byteBuffer;

  public StringProduceRecord(String record) {
    this.partitionKey = "key-" + record.hashCode() + "-" + DateTime.now().toString();
    this.record = record;
    this.byteBuffer = Charset.forName("UTF-8").encode(CharBuffer.wrap(record));
  }

  @Override
  public String getPartitionKey() {
    return this.partitionKey;
  }

  @Override
  public String getRecord() {
    return this.record;
  }

  @Override
  public ByteBuffer getByteBuffer() {
    return this.byteBuffer;
  }

  @Override
  public String getString() {
    return this.toString();
  }

  @Override
  public String toString() {
    return "StringProduceRecord{" +
      "partitionKey='" + partitionKey + '\'' +
      ", record='" + record + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StringProduceRecord that = (StringProduceRecord) o;
    return Objects.equals(partitionKey, that.partitionKey) &&
      Objects.equals(record, that.record);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionKey, record);
  }
}
