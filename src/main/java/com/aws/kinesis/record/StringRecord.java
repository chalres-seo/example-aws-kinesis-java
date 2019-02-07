package com.aws.kinesis.record;

import com.amazonaws.services.kinesis.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

/**
 * @throws CharacterCodingException
 */
public class StringRecord implements IRecord<String> {
  private static final Logger logger = LoggerFactory.getLogger(StringRecord.class);

  private static final CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
  private static final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();

  private final String partitionKey;
  private final ByteBuffer data;
  private final String value;
  private final String sequenceNumber;

  private StringRecord(String partitionKey, ByteBuffer data, String value, String sequenceNumber) {
    this.partitionKey = partitionKey;
    this.data = data;
    this.value = value;
    this.sequenceNumber = sequenceNumber;
  }

  public StringRecord(String partitionKey, ByteBuffer data, String sequenceNumber) throws CharacterCodingException {
    this(partitionKey,
      data,
      byteBufferToString(data),
      sequenceNumber);
  }

  public StringRecord(String partitionKey, String value, String sequenceNumber) throws CharacterCodingException {
    this(partitionKey,
      stringToByteBuffer(value),
      value,
      sequenceNumber);
  }

  public StringRecord(String partitionKey, ByteBuffer data) throws CharacterCodingException {
    this(partitionKey,
      data,
      byteBufferToString(data),
      null);
  }

  public StringRecord(String partitionKey, String value) throws CharacterCodingException {
    this(partitionKey,
      stringToByteBuffer(value),
      value,
      null);
  }

  public StringRecord(Record kinesisRecord) throws CharacterCodingException {
    this(kinesisRecord.getPartitionKey(),
      kinesisRecord.getData(),
      kinesisRecord.getSequenceNumber());
  }

  @Override
  public String getPartitionKey() {
    return this.partitionKey;
  }

  @Override
  public String getValue() {
    return this.value;
  }

  @Override
  public ByteBuffer getData() { return this.data; }

  @Override
  public Optional<String> getSequenceNumber() {
    return Optional.ofNullable(sequenceNumber);
  }

  @Override
  public String toString() {
    return "StringRecord{" +
      "partitionKey='" + partitionKey + '\'' +
      ", data=" + data +
      ", value='" + value + '\'' +
      ", sequenceNumber='" + sequenceNumber + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    StringRecord that = (StringRecord) o;

    return
      this.partitionKey.equals(that.getPartitionKey()) &&
      this.getData().equals(that.getData()) &&
      this.getSequenceNumber().equals(that.getSequenceNumber()) &&
      this.getValue().equals(that.getValue());
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionKey, data, sequenceNumber, data);
  }

  private static ByteBuffer stringToByteBuffer(String string) throws CharacterCodingException {
    return encoder.encode(CharBuffer.wrap(string));
  }

  private static String byteBufferToString(ByteBuffer byteBuffer) throws CharacterCodingException {
    final ByteBuffer readOnlyByteBuffer = byteBuffer.asReadOnlyBuffer();
    readOnlyByteBuffer.rewind();

    return decoder.decode(readOnlyByteBuffer).toString();
  }
}